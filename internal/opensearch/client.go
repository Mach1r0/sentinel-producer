package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Mach1r0/sentinel-producer/internal/event"
)

type Config struct {
	URL         string
	IndexPrefix string
	HTTPClient  *http.Client
}

type Client struct {
	baseURL     string
	indexPrefix string
	httpClient  *http.Client
}

func NewClient(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("opensearch URL is required")
	}

	if strings.TrimSpace(cfg.IndexPrefix) == "" {
		return nil, errors.New("opensearch index prefix is required")
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &Client{
		baseURL:     strings.TrimRight(strings.TrimSpace(cfg.URL), "/"),
		indexPrefix: strings.TrimSpace(cfg.IndexPrefix),
		httpClient:  httpClient,
	}, nil
}

func (c *Client) InstallTemplate(
	ctx context.Context,
	template []byte,
) error {
	if len(template) == 0 {
		return errors.New("opensearch template is required")
	}

	endpoint := fmt.Sprintf(
		"%s/_index_template/%s",
		c.baseURL,
		url.PathEscape(c.indexPrefix),
	)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		endpoint,
		bytes.NewReader(template),
	)
	if err != nil {
		return fmt.Errorf("create template request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("install opensearch template: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf(
			"install opensearch template: status %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}

	return nil
}

func (c *Client) Index(
	ctx context.Context,
	securityEvent event.Event,
) error {
	if err := securityEvent.Validate(); err != nil {
		return fmt.Errorf("validate security event: %w", err)
	}

	document, err := json.Marshal(securityEvent)
	if err != nil {
		return fmt.Errorf("serialize security event: %w", err)
	}

	indexName := fmt.Sprintf(
		"%s-%s",
		c.indexPrefix,
		securityEvent.Timestamp.Format("2006.01.02"),
	)
	endpoint := fmt.Sprintf(
		"%s/%s/_doc/%s",
		c.baseURL,
		url.PathEscape(indexName),
		url.PathEscape(securityEvent.ID),
	)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		endpoint,
		bytes.NewReader(document),
	)
	if err != nil {
		return fmt.Errorf("create indexing request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("index security event: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK &&
		response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf(
			"index security event: status %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}

	return nil
}
