package sampling

import (
	"math"
	"math/rand/v2"
	"regexp"
	"strings"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/time/rate"

	"github.com/Jenil133/Shadowtap/internal/tracectx"
)

// Sampler makes sampling decisions. It is safe for concurrent use.
type Sampler struct {
	salt        string
	exclude     map[string]struct{}
	rules       []compiledRule
	defaultRate float64
	fallback    threshold
	limiter     *rate.Limiter // nil when unlimited
}

type compiledRule struct {
	name     string
	services map[string]struct{}
	methods  map[string]struct{}
	prefix   string
	re       *regexp.Regexp
	rate     float64
	th       threshold
}

// threshold admits a 64-bit hash when it falls below the rate's share of the
// hash space. Because thresholds are monotonic in the rate, a request sampled
// at rate r is sampled at every rate above r.
type threshold struct {
	all   bool
	limit uint64
}

func newThreshold(r float64) threshold {
	const space = 1 << 64 // as float64: 2^64
	if r >= 1 {
		return threshold{all: true}
	}
	if r <= 0 {
		return threshold{}
	}
	t := r * space
	if t >= math.MaxUint64 {
		return threshold{all: true}
	}
	return threshold{limit: uint64(t)}
}

func (t threshold) admit(h uint64) bool { return t.all || h < t.limit }

// New validates cfg and returns a Sampler.
func New(cfg Config) (*Sampler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s := &Sampler{
		salt:        cfg.Salt,
		exclude:     make(map[string]struct{}, len(cfg.ExcludePaths)),
		defaultRate: cfg.DefaultRate,
		fallback:    newThreshold(cfg.DefaultRate),
	}
	for _, p := range cfg.ExcludePaths {
		s.exclude[p] = struct{}{}
	}
	for _, r := range cfg.Rules {
		cr := compiledRule{name: r.Name, prefix: r.PathPrefix, rate: r.Rate, th: newThreshold(r.Rate)}
		if len(r.Services) > 0 {
			cr.services = make(map[string]struct{}, len(r.Services))
			for _, svc := range r.Services {
				cr.services[svc] = struct{}{}
			}
		}
		if len(r.Methods) > 0 {
			cr.methods = make(map[string]struct{}, len(r.Methods))
			for _, m := range r.Methods {
				cr.methods[strings.ToUpper(m)] = struct{}{}
			}
		}
		if r.PathRegex != "" {
			cr.re = regexp.MustCompile(r.PathRegex) // validated above
		}
		s.rules = append(s.rules, cr)
	}
	if cfg.MaxPerSecond > 0 {
		burst := cfg.Burst
		if burst == 0 {
			burst = max(1, int(math.Ceil(cfg.MaxPerSecond)))
		}
		s.limiter = rate.NewLimiter(rate.Limit(cfg.MaxPerSecond), burst)
	}
	return s, nil
}

// Decide returns the sampling decision for r.
func (s *Sampler) Decide(r Request) Decision {
	get := r.Header
	if get == nil {
		get = func(string) string { return "" }
	}
	path := r.Path
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	if strings.EqualFold(r.Method, "CONNECT") || get("upgrade") != "" {
		return Decision{Reason: ReasonExcluded, Rule: RuleUpgrade}
	}
	if _, ok := s.exclude[path]; ok {
		return Decision{Reason: ReasonExcluded, Rule: RuleExcludePath}
	}

	d := Decision{Rule: RuleDefault, Reason: ReasonUnsampled, Rate: s.defaultRate}
	th := s.fallback
	method := strings.ToUpper(r.Method)
	for i := range s.rules {
		if s.rules[i].matches(r.Service, method, path) {
			d.Rule, d.Rate, th = s.rules[i].name, s.rules[i].rate, s.rules[i].th
			break
		}
	}

	var h uint64
	if tc, ok := tracectx.FromHeaders(get); ok {
		h, d.KeySource = s.hash(tc.TraceID), tc.Source
	} else if id := get("x-request-id"); id != "" {
		h, d.KeySource = s.hash(id), KeySourceRequestID
	} else {
		h, d.KeySource = rand.Uint64(), KeySourceRandom
	}
	if !th.admit(h) {
		return d
	}
	if s.limiter != nil && !s.limiter.Allow() {
		d.Reason = ReasonRateLimited
		return d
	}
	d.Sampled, d.Reason = true, ReasonSampled
	return d
}

func (s *Sampler) hash(key string) uint64 {
	if s.salt == "" {
		return xxhash.Sum64String(key)
	}
	return xxhash.Sum64String(s.salt + key)
}

func (r *compiledRule) matches(service, method, path string) bool {
	if r.services != nil {
		if _, ok := r.services[service]; !ok {
			return false
		}
	}
	if r.methods != nil {
		if _, ok := r.methods[method]; !ok {
			return false
		}
	}
	if r.prefix != "" && !strings.HasPrefix(path, r.prefix) {
		return false
	}
	return r.re == nil || r.re.MatchString(path)
}
