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
	endpoint string
	client   *http.Client
}

// NewRepo creates a new representation of a RDF repository. It takes a
// variadic list of functional options which can alter the configuration
// of the repository.
func NewRepo(addr string, options ...func(*Repo) error) (*Repo, error) {
	r := Repo{
		endpoint: addr,
		client:   http.DefaultClient,
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
func (r *Repo) ConstructFormat(query string, format string) (string, error) {
	form := url.Values{}

	if strings.Contains(query, "INSERT") || strings.Contains(query, "DELETE") {
		form.Set("request", query)
	} else {
		form.Set("query", query)
		form.Set("format", format)
	}

	b := form.Encode()

	req, err := http.NewRequest(
		"POST",
		r.endpoint,
		bytes.NewBufferString(b))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Content-Length", strconv.Itoa(len(b)))
	req.Header.Set("Accept", format)

	resp, err := r.client.Do(req)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("Construct: SPARQL request failed: %s. "+msg, resp.Status)
	}
	res, err := ioutil.ReadAll(resp.Body)
	return string(res), err
}
