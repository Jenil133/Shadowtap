package scrub

// Defaults returns the built-in protections: sensitive field names and name
// suffixes, credential, IP, URL and drop header lists, the media types
// scrubbed as text, and the decompression cap. Key is nil. Lists given to
// New are added to these; every call returns fresh slices.
func Defaults() Config {
	return Config{
		SensitiveKeys: []string{
			"password", "passwd", "pwd", "secret", "token", "accesstoken", "refreshtoken",
			"idtoken", "apikey", "apisecret", "clientsecret", "privatekey", "auth",
			"authorization", "cookie", "session", "sessionid", "ssn", "socialsecuritynumber",
			"taxid", "nationalid", "passport", "passportnumber", "driverslicense",
			"creditcard", "cardnumber", "ccnumber", "pan", "cvv", "cvc", "cvv2",
			"securitycode", "pin", "iban", "accountnumber", "routingnumber", "dob",
			"dateofbirth", "birthdate", "birthday", "email", "emailaddress", "phone",
			"phonenumber", "mobile", "mobilenumber", "firstname", "lastname", "fullname",
			"middlename", "surname", "givenname", "address", "addressline1", "addressline2",
			"street", "streetaddress", "postalcode", "zip", "zipcode", "ip", "ipaddress",
			"latitude", "longitude", "lat", "lng", "lon",
			// Plurals and variants of the above, and other credentials and
			// identifiers APIs commonly carry.
			"phones", "emails", "addresses", "mobiles", "username", "displayname",
			"nickname", "otp", "authtoken", "sessiontoken", "bearertoken", "apitoken",
			"csrftoken", "xsrftoken", "secretkey", "secretaccesskey", "accesskeyid",
			"securitytoken", "privatetoken", "subscriptionkey", "clientip", "remoteip",
			"remoteaddr", "sourceip", "geo", "coordinates", "geolocation",
		},
		// No "token" or "ip" suffix: pageToken and relationship are not
		// personal.
		SensitiveKeySuffixes: []string{
			"email", "emailaddress", "phone", "phonenumber", "password", "passwd", "ssn",
			"cardnumber", "creditcard", "cvv", "address", "streetaddress", "firstname",
			"lastname", "fullname", "dateofbirth", "birthdate", "accesstoken", "refreshtoken",
			"idtoken", "apikey", "secret", "clientsecret", "sessionid", "ipaddress",
			"postalcode", "zipcode", "username", "authtoken", "sessiontoken", "bearertoken",
			"apitoken", "csrftoken", "secretkey", "privatetoken", "subscriptionkey",
			"phones", "emails",
		},
		CredentialHeaders: []string{
			"authorization", "proxy-authorization", "x-api-key", "x-auth-token",
			"x-csrf-token", "x-xsrf-token", "x-amz-security-token", "x-access-token",
			"x-session-token", "api-key", "x-goog-api-key", "private-token",
			"ocp-apim-subscription-key", "x-gitlab-token", "x-vault-token",
		},
		IPHeaders: []string{
			"x-forwarded-for", "x-real-ip", "true-client-ip", "cf-connecting-ip",
			"x-envoy-external-address", "x-client-ip", "forwarded", "x-original-forwarded-for",
			"cf-connecting-ipv6", "x-cluster-client-ip", "fastly-client-ip", "client-ip",
			"x-originating-ip", "x-remote-ip", "x-remote-addr", "remote-addr",
		},
		URLHeaders:  []string{"referer", "location", "content-location"},
		DropHeaders: []string{"x-envoy-peer-metadata", "x-envoy-peer-metadata-id"},
		TextContentTypes: []string{
			"text/plain", "text/html", "text/csv", "text/event-stream", "application/graphql",
		},
		MaxDecodedBytes: DefaultMaxDecodedBytes,
	}
}
