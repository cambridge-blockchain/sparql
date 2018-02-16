package sparql

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/knakk/digest"
	"github.com/knakk/rdf"
)

// Repo represent a RDF repository, assumed to be
// queryable via the SPARQL protocol over HTTP.
type Repo struct {
	client   *http.Client
	dbType   string
	endpoint string
}

// NewRepo creates a new representation of a RDF repository. It takes a
// variadic list of functional options which can alter the configuration
// of the repository.
func NewRepo(addr string, dbType string, options ...func(*Repo) error) (*Repo, error) {
	r := Repo{
		client:   http.DefaultClient,
		dbType:   dbType,
		endpoint: addr,
	}
	return &r, r.SetOption(options...)
}

// SetOption takes one or more option function and applies them in order to Repo.
func (r *Repo) SetOption(options ...func(*Repo) error) error {
	for _, opt := range options {
		if err := opt(r); err != nil {
			return err
		}
	}
	return nil
}

// DigestAuth configures Repo to use digest authentication on HTTP requests.
func DigestAuth(username, password string) func(*Repo) error {
	return func(r *Repo) error {
		r.client.Transport = digest.NewTransport(username, password)
		return nil
	}
}

// Timeout instructs the underlying HTTP transport to timeout after given duration.
func Timeout(t time.Duration) func(*Repo) error {
	return func(r *Repo) error {
		r.client.Timeout = t
		return nil
	}
}

// Query performs a SPARQL HTTP request to the Repo, and returns the
// parsed application/sparql-results+json response.
func (r *Repo) Query(q string) (*Results, error) {
	form := url.Values{}
	form.Set("query", q)
	b := form.Encode()

	// TODO make optional GET or Post, Query() should default GET (idempotent, cacheable)
	// maybe new for updates: func (r *Repo) Update(q string) using POST?
	req, err := http.NewRequest(
		"POST",
		r.endpoint,
		bytes.NewBufferString(b))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Content-Length", strconv.Itoa(len(b)))
	req.Header.Set("Accept", "application/sparql-results+json")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err2 := ioutil.ReadAll(resp.Body)
		var msg string
		if err2 != nil {
			msg = "Failed to read response body"
		} else {
			if strings.TrimSpace(string(b)) != "" {
				msg = "Response body: \n" + string(b)
			}
		}
		return nil, fmt.Errorf("Query: SPARQL request failed: %s. "+msg, resp.Status)
	}
	results, err := ParseJSON(resp.Body)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Construct performs a SPARQL HTTP request to the Repo, and returns the
// result triples.
func (r *Repo) Construct(q string) ([]rdf.Triple, error) {
	res, err := r.ConstructFormat(q, "text/turtle")
	if err != nil {
		return nil, err
	}
	dec := rdf.NewTripleDecoder(bytes.NewBufferString(res), rdf.Turtle)
	return dec.DecodeAll()
}

// ConstructFormat performs a SPARQL HTTP request to the Repo, and returns the
// result as string. It accepts as input one of the following Accept header values:
//    - text/turtle
//    - application/n-quads
//    - application/rdf+xml
//    - application/trix
//    - application/x-trig
//    - text/rdf+n3
//    - application/rdf+json
//    - application/x-binary-rdf
//    - text/plain
func (r *Repo) ConstructFormat(query string, format string) (response string, err error) {
	var (
		clientReq  *http.Request
		clientRes  *http.Response
		form       url.Values
		buf        *bytes.Buffer
		res        []byte
		httpMethod string
		reqURL     string
	)

	form = url.Values{}

	if r.dbType == "ontotext" {
		if strings.Contains(query, "INSERT") || strings.Contains(query, "DELETE") {
			form.Set("update", query)

			httpMethod = "POST"
			buf = bytes.NewBufferString(form.Encode())
		} else {
			form.Set("query", query)

			httpMethod = "GET"
			buf = bytes.NewBuffer(nil)
		}

		reqURL = fmt.Sprintf("%s?%s", r.endpoint, form.Encode())
	} else if r.dbType == "oracle" {
		if strings.Contains(query, "INSERT") || strings.Contains(query, "DELETE") {
			form.Set("request", query)
		} else {
			form.Set("query", query)
			form.Set("format", format)
		}

		httpMethod = "POST"
		reqURL = r.endpoint
		buf = bytes.NewBufferString(form.Encode())
	} else {
		return "", fmt.Errorf("Invalid database type: %s", r.dbType)
	}

	if clientReq, err = http.NewRequest(httpMethod, reqURL, buf); err != nil {
		return "", err
	}

	if r.dbType == "oracle" {
		clientReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	clientReq.Header.Set("Content-Length", strconv.Itoa(len(form.Encode())))
	clientReq.Header.Set("Accept", format)

	if clientRes, err = r.client.Do(clientReq); err != nil {
		return "", err
	}

	defer clientRes.Body.Close()

	if clientRes.StatusCode < 200 || clientRes.StatusCode > 205 {
		if res, err = ioutil.ReadAll(clientRes.Body); err != nil {
			return "", fmt.Errorf(
				"Construct: SPARQL request failed: %s. Failed to read response body",
				clientRes.Status,
			)
		}

		if strings.TrimSpace(string(res)) != "" {
			return "", fmt.Errorf(
				"Construct: SPARQL request failed: %s. Response body: \n %s",
				clientRes.Status,
				string(res),
			)
		}
	}

	if res, err = ioutil.ReadAll(clientRes.Body); err != nil {
		return "", err
	}

	response = string(res)

	return
}
