package index

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
)

// templateFS holds the ILM policy, component templates and index templates
// Bootstrap applies. The files are plain Elasticsearch API bodies, so they
// can also be applied by hand with curl.
//
//go:embed templates/*.json
var templateFS embed.FS

// ILMPolicy is the name of the lifecycle policy every data stream uses:
// roll over daily or at 10 GB per shard, delete after 7 days.
const ILMPolicy = "shadowtap"

// Template kinds, which decide the API a template is applied with.
const (
	KindILMPolicy         = "ILM policy"
	KindComponentTemplate = "component template"
	KindIndexTemplate     = "index template"
)

// Template is one embedded Elasticsearch resource.
type Template struct {
	Kind string // KindILMPolicy, KindComponentTemplate or KindIndexTemplate
	Name string // resource name in Elasticsearch
	File string // file under templates/
}

// templates lists the embedded resources in the order they must be applied:
// the policy first, then the component templates the index templates
// compose, then the index templates the data streams need.
var templates = []Template{
	{KindILMPolicy, ILMPolicy, "ilm-shadowtap.json"},
	{KindComponentTemplate, "shadowtap-settings", "component-shadowtap-settings.json"},
	{KindComponentTemplate, "shadowtap-common", "component-shadowtap-common.json"},
	{KindIndexTemplate, "traces-shadowtap", "index-traces-shadowtap.json"},
	{KindIndexTemplate, "logs-shadowtap", "index-logs-shadowtap.json"},
	{KindIndexTemplate, "shadowtap-captures", "index-shadowtap-captures.json"},
	{KindIndexTemplate, "shadowtap-diffs", "index-shadowtap-diffs.json"},
}

// Templates returns the embedded resources in the order Bootstrap applies
// them, with their JSON bodies.
func Templates() ([]Template, map[Template][]byte, error) {
	bodies := make(map[Template][]byte, len(templates))
	for _, t := range templates {
		body, err := templateFS.ReadFile("templates/" + t.File)
		if err != nil {
			return nil, nil, fmt.Errorf("index: embedded template %s: %w", t.File, err)
		}
		bodies[t] = body
	}
	return append([]Template(nil), templates...), bodies, nil
}

// ESError is an Elasticsearch response with an error status.
type ESError struct {
	Status int
	// Body is the start of the response body, usually a JSON error object.
	Body string
}

func (e *ESError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("elasticsearch returned %d", e.Status)
	}
	return fmt.Sprintf("elasticsearch returned %d: %s", e.Status, e.Body)
}

// maxErrorBody bounds how much of an error response is kept.
const maxErrorBody = 4096

// Bootstrap makes Elasticsearch ready for the indexer: it puts the ILM
// policy, the component templates and the index templates (a PUT replaces
// an existing resource with the same definition, so repeating it is
// harmless) and creates the data streams named by names when they do not
// exist. It is safe to run from several replicas at once and on every
// start. Tests of other commands can call it against a fake Elasticsearch to
// learn the mappings.
func Bootstrap(ctx context.Context, es *elasticsearch.Client, names Indices) error {
	if es == nil {
		return errors.New("index: nil elasticsearch client")
	}
	order, bodies, err := Templates()
	if err != nil {
		return err
	}
	for _, t := range order {
		body := bodies[t]
		var (
			res *esapi.Response
			err error
		)
		switch t.Kind {
		case KindILMPolicy:
			res, err = es.ILM.PutLifecycle(bytes.NewReader(body), t.Name, es.ILM.PutLifecycle.WithContext(ctx))
		case KindComponentTemplate:
			res, err = es.Cluster.PutComponentTemplate(t.Name, bytes.NewReader(body), es.Cluster.PutComponentTemplate.WithContext(ctx))
		case KindIndexTemplate:
			res, err = es.Indices.PutIndexTemplate(t.Name, bytes.NewReader(body), es.Indices.PutIndexTemplate.WithContext(ctx))
		}
		if err != nil {
			return fmt.Errorf("index: put %s %s: %w", t.Kind, t.Name, err)
		}
		if status, body := drain(res); status < 200 || status >= 300 {
			return fmt.Errorf("index: put %s %s: %w", t.Kind, t.Name, &ESError{Status: status, Body: body})
		}
	}
	for _, name := range names.All() {
		if err := ensureDataStream(ctx, es, name); err != nil {
			return err
		}
	}
	return nil
}

// ensureDataStream creates the data stream unless it exists. Two replicas
// bootstrapping at once can both see it missing; the loser's create then
// fails with resource_already_exists_exception, which is success here.
func ensureDataStream(ctx context.Context, es *elasticsearch.Client, name string) error {
	res, err := es.Indices.GetDataStream(es.Indices.GetDataStream.WithName(name), es.Indices.GetDataStream.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("index: get data stream %s: %w", name, err)
	}
	status, body := drain(res)
	switch {
	case status == http.StatusOK:
		return nil
	case status != http.StatusNotFound:
		return fmt.Errorf("index: get data stream %s: %w", name, &ESError{Status: status, Body: body})
	}
	res, err = es.Indices.CreateDataStream(name, es.Indices.CreateDataStream.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("index: create data stream %s: %w", name, err)
	}
	status, body = drain(res)
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusBadRequest && strings.Contains(body, "resource_already_exists_exception"):
		return nil
	}
	return fmt.Errorf("index: create data stream %s: %w", name, &ESError{Status: status, Body: body})
}

// drain reads the start of a response body and closes it, so the connection
// returns to the pool.
func drain(res *esapi.Response) (status int, body string) {
	if res == nil {
		return 0, ""
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBody))
	_, _ = io.Copy(io.Discard, res.Body)
	return res.StatusCode, strings.TrimSpace(string(b))
}
