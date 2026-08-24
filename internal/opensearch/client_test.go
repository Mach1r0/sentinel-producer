package opensearch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
)

func TestNewClientRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "empty URL", cfg: Config{IndexPrefix: "security-events"}},
		{name: "empty index prefix", cfg: Config{URL: "http://localhost:9200"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewClient(test.cfg); err == nil {
				t.Fatal("expected an invalid configuration error")
			}
		})
	}
}

func TestInstallTemplate(t *testing.T) {
	template := []byte(`{"index_patterns":["security-events-*"]}`)

	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", request.Method)
		}
		if request.URL.Path != "/_index_template/security-events" {
			t.Errorf("unexpected request path %q", request.URL.Path)
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected content type %q", request.Header.Get("Content-Type"))
		}

		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if string(body) != string(template) {
			t.Errorf("expected body %q, got %q", template, body)
		}

		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient(t, server)
	if err := client.InstallTemplate(context.Background(), template); err != nil {
		t.Fatalf("install template: %v", err)
	}
}

func TestInstallTemplateRejectsEmptyTemplate(t *testing.T) {
	client, err := NewClient(Config{
		URL:         "http://localhost:9200",
		IndexPrefix: "security-events",
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	if err := client.InstallTemplate(context.Background(), nil); err == nil {
		t.Fatal("expected an empty template error")
	}
}

func TestIndexEvent(t *testing.T) {
	expected := validTestEvent()

	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", request.Method)
		}
		expectedPath := "/security-events-2026.08.23/_doc/test-event-001"
		if request.URL.Path != expectedPath {
			t.Errorf("expected path %q, got %q", expectedPath, request.URL.Path)
		}

		var received event.Event
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if received.ID != expected.ID {
			t.Errorf("expected event ID %q, got %q", expected.ID, received.ID)
		}

		writer.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := newTestClient(t, server)
	if err := client.Index(context.Background(), expected); err != nil {
		t.Fatalf("index event: %v", err)
	}
}

func TestIndexRejectsInvalidEvent(t *testing.T) {
	client, err := NewClient(Config{
		URL:         "http://localhost:9200",
		IndexPrefix: "security-events",
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	if err := client.Index(context.Background(), event.Event{}); err == nil {
		t.Fatal("expected an invalid event error")
	}
}

func TestIndexReturnsOpenSearchError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		_ *http.Request,
	) {
		http.Error(writer, "index unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newTestClient(t, server)
	err := client.Index(context.Background(), validTestEvent())
	if err == nil {
		t.Fatal("expected an OpenSearch error")
	}
	if !strings.Contains(err.Error(), "index unavailable") {
		t.Fatalf("expected response body in error, got %v", err)
	}
}

func newTestClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()

	client, err := NewClient(Config{
		URL:         server.URL,
		IndexPrefix: "security-events",
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	return client
}

func validTestEvent() event.Event {
	return event.Event{
		ID:        "test-event-001",
		Timestamp: time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC),
		EventType: "auth_failed",
		SourceIP:  "10.0.0.15",
		Severity:  "high",
		Raw:       json.RawMessage(`{"user":"admin","attempts":5}`),
	}
}
