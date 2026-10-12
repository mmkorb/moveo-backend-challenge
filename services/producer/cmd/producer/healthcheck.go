package main

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/config"
)

// healthcheck probes GET /readyz on loopback and returns an error for any
// connection failure, timeout or non-200 answer. The distroless image has no
// curl, so Docker's HEALTHCHECK and Compose run this subcommand (ADR 0013).
func healthcheck(ctx context.Context) error {
	url, err := config.HealthcheckURL()
	if err != nil {
		return err
	}
	return probe(ctx, http.DefaultClient, url)
}

func probe(ctx context.Context, client *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build healthcheck request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer func() { _ = resp.Body.Close() }() // read-only response, nothing to lose on a failed close
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s answered %s", url, resp.Status)
	}
	return nil
}
