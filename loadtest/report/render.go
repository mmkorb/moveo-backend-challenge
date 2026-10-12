package main

import (
	"bytes"
	"embed"
	"fmt"
	htmltemplate "html/template"
	"os"
	"strconv"
	"strings"
	texttemplate "text/template"
)

//go:embed report.tmpl.md report.tmpl.html
var templates embed.FS

// templateFuncs returns the functions shared by both templates.
func templateFuncs() map[string]any {
	return map[string]any{
		// cell escapes a value for a Markdown table cell.
		"cell": func(s string) string {
			s = strings.ReplaceAll(s, "|", `\|`)
			return strings.ReplaceAll(s, "\n", " ")
		},
		"lower": strings.ToLower,
		// grp adds thousands separators to an integer string (CSV cells).
		"grp": func(s string) string {
			v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
			if err != nil {
				return s
			}
			return groupThousands(v)
		},
	}
}

func renderMarkdown(r *report) ([]byte, error) {
	t, err := texttemplate.New("report.tmpl.md").Funcs(templateFuncs()).ParseFS(templates, "report.tmpl.md")
	if err != nil {
		return nil, fmt.Errorf("parse markdown template: %w", err)
	}
	var b bytes.Buffer
	if err := t.Execute(&b, r); err != nil {
		return nil, fmt.Errorf("render markdown: %w", err)
	}
	return b.Bytes(), nil
}

func renderHTML(r *report) ([]byte, error) {
	t, err := htmltemplate.New("report.tmpl.html").Funcs(templateFuncs()).ParseFS(templates, "report.tmpl.html")
	if err != nil {
		return nil, fmt.Errorf("parse html template: %w", err)
	}
	var b bytes.Buffer
	if err := t.Execute(&b, r); err != nil {
		return nil, fmt.Errorf("render html: %w", err)
	}
	return b.Bytes(), nil
}

// writeReports renders both formats before writing either, so a template
// error never leaves a half-written pair behind.
func writeReports(r *report, mdPath, htmlPath string) error {
	md, err := renderMarkdown(r)
	if err != nil {
		return err
	}
	html, err := renderHTML(r)
	if err != nil {
		return err
	}
	if err := os.WriteFile(mdPath, md, 0o644); err != nil { //nolint:gosec // G306: reports are meant to be read by other users and tools
		return fmt.Errorf("write %s: %w", mdPath, err)
	}
	if err := os.WriteFile(htmlPath, html, 0o644); err != nil { //nolint:gosec // G306: reports are meant to be read by other users and tools
		return fmt.Errorf("write %s: %w", htmlPath, err)
	}
	return nil
}
