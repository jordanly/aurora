// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package client talks to the original Aurora Thrift JSON API. It neither
// schedules tasks nor interprets executable configuration languages.
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const MaxBytes = 1 << 20

// Struct is the numeric-field representation defined by Thrift TJSONProtocol.
type Struct map[string]any

func Field(kind string, value any) Struct { return Struct{kind: value} }
func Set(kind string, values ...any) []any {
	return append([]any{kind, len(values)}, values...)
}
func Get(value any, number int) any {
	m, ok := value.(map[string]any)
	if !ok {
		if s, typed := value.(Struct); typed {
			m = s
		} else {
			return nil
		}
	}
	f, ok := m[strconv.Itoa(number)].(map[string]any)
	if !ok {
		if s, typed := m[strconv.Itoa(number)].(Struct); typed {
			f = s
		} else {
			return nil
		}
	}
	if len(f) != 1 {
		return nil
	}
	for _, v := range f {
		return v
	}
	return nil
}

// Decode rejects unknown configuration fields, duplicate keys, trailing JSON,
// excessive depth and overlarge input before interpreting a document.
func Decode(data []byte, value any) error {
	if len(data) > MaxBytes {
		return errors.New("JSON exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := uniqueValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	if err := exactSchema(data, reflect.TypeOf(value)); err != nil {
		return err
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

// Typed configuration uses exact JSON field names. Omitted fields retain their
// defaults; explicit null is rejected instead of silently retaining or clearing
// a default. Untyped RPC arguments and replies retain JSON's native null values.
func exactSchema(data json.RawMessage, target reflect.Type) error {
	if target == nil {
		return nil
	}
	for target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if target.Kind() == reflect.Interface {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("explicit null is not allowed in typed configuration; omit optional fields")
	}
	switch target.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return err
		}
		allowed := map[string]reflect.Type{}
		for i := 0; i < target.NumField(); i++ {
			field := target.Field(i)
			if !field.IsExported() {
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			allowed[name] = field.Type
		}
		for name, raw := range fields {
			field, ok := allowed[name]
			if !ok {
				return fmt.Errorf("unknown JSON field %q", name)
			}
			if err := exactSchema(raw, field); err != nil {
				return fmt.Errorf("field %q: %w", name, err)
			}
		}
	case reflect.Map:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return err
		}
		for name, raw := range fields {
			if err := exactSchema(raw, target.Elem()); err != nil {
				return fmt.Errorf("map entry %q: %w", name, err)
			}
		}
	case reflect.Slice, reflect.Array:
		var values []json.RawMessage
		if err := json.Unmarshal(data, &values); err != nil {
			return err
		}
		for i, raw := range values {
			if err := exactSchema(raw, target.Elem()); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
		}
	}
	return nil
}

func uniqueValue(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("JSON nesting exceeds 64")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isContainer := token.(json.Delim)
	if !isContainer {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("invalid or duplicate JSON key %q", key)
			}
			seen[name] = true
			if err = uniqueValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := uniqueValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}

func ReadJSON(path string, input io.Reader, value any) error {
	if path != "-" {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		input = file
	}
	data, err := io.ReadAll(io.LimitReader(input, MaxBytes+1))
	if err != nil {
		return err
	}
	return Decode(data, value)
}

type Connection struct {
	URL             string
	CAFile          string
	CertificateFile string
	KeyFile         string
	Timeout         time.Duration
}

type Client struct {
	endpoint string
	http     *http.Client
	sequence atomic.Int64
}

func New(config Connection) (*Client, error) {
	u, err := url.Parse(config.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("scheduler must be an http(s) URL without credentials, query or fragment")
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/api"
	}
	if u.Path != "/api" {
		return nil, errors.New("scheduler URL path must be /api")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("request timeout must be positive")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if config.CAFile != "" {
		data, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, errors.New("CA file contains no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if (config.CertificateFile == "") != (config.KeyFile == "") {
		return nil, errors.New("client certificate and key must be supplied together")
	}
	if config.CertificateFile != "" {
		pair, err := tls.LoadX509KeyPair(config.CertificateFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}
	if u.Scheme != "https" && (config.CAFile != "" || config.CertificateFile != "") {
		return nil, errors.New("TLS credentials require an https scheduler URL")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = tlsConfig
	return &Client{endpoint: u.String(), http: &http.Client{Transport: transport,
		Timeout: config.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("scheduler redirect refused; specify its leader URL")
		}}}, nil
}

// Call never retries a mutation: after a lost response, query scheduler state
// before deciding whether another invocation is safe.
func (c *Client) Call(ctx context.Context, method string, args Struct) (Struct, error) {
	if method == "" || strings.IndexFunc(method, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
	}) >= 0 {
		return nil, errors.New("invalid Thrift method name")
	}
	sequence := c.sequence.Add(1)
	// Compact output is required by the retained Thrift 0.10 JSON reader.
	body, err := json.Marshal([]any{1, method, 1, sequence, args})
	if err != nil {
		return nil, err
	}
	if len(body) > MaxBytes {
		return nil, errors.New("API request exceeds 1 MiB")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/x-thrift")
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scheduler HTTP status %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, MaxBytes+1))
	if err != nil {
		return nil, err
	}
	var wire []any
	if err = Decode(raw, &wire); err != nil {
		return nil, fmt.Errorf("invalid Thrift reply: %w", err)
	}
	if len(wire) != 5 || wire[0] != json.Number("1") || wire[1] != method ||
		wire[3] != json.Number(strconv.FormatInt(sequence, 10)) {
		return nil, errors.New("Thrift reply method/version/sequence mismatch")
	}
	if wire[2] == json.Number("3") {
		return nil, fmt.Errorf("Thrift application exception: %v", Get(wire[4], 1))
	}
	if wire[2] != json.Number("2") {
		return nil, errors.New("Thrift reply is not a response")
	}
	result, ok := Get(wire[4], 0).(map[string]any)
	if !ok {
		return nil, errors.New("Thrift reply has no success result")
	}
	if Get(result, 1) != json.Number("1") {
		details, _ := json.Marshal(Get(result, 6))
		return Struct(result), fmt.Errorf("%s rejected (code %v): %s", method, Get(result, 1), details)
	}
	return Struct(result), nil
}
