// Package migrations embeds the goose SQL migrations (ADR 0010). The consumer
// binary carries them, so the image needs no extra tool and no mounted files.
package migrations

import "embed"

// FS holds the versioned *.sql files at its root.
//
//go:embed *.sql
var FS embed.FS
