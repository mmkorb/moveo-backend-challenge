package postgres

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// transient reports whether err is worth retrying (ADR 0011): SQLSTATE classes
// 08 (connection), 40 (serialization, deadlock), 53 (insufficient resources) and
// 57 (operator intervention, including 57014 statement timeout), connect and
// network errors, and timeouts. A server error outside these classes (CHECK
// violation, missing grant, missing table...) is "unknown": it is not wrapped,
// the partition worker still retries it, but logs it at ERROR.
func transient(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if len(pgErr.Code) < 2 {
			return false
		}
		switch pgErr.Code[:2] {
		case "08", "40", "53", "57":
			return true
		}
		return false
	}
	var connErr *pgconn.ConnectError
	var netErr net.Error
	return errors.As(err, &connErr) ||
		errors.As(err, &netErr) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		pgconn.Timeout(err)
}

// dataException reports whether err is a server error of SQLSTATE class 22
// (data exception). Retrying cannot fix it, and it cannot be told apart from a
// bug by the adapter, so it gets its own sentinel but no DLQ routing (ADR 0011).
func dataException(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22")
}

// wrap adds the operation as context, ports.ErrUnavailable for transient errors
// and ports.ErrDataException for SQLSTATE class 22 (ADR 0011, 0010). The driver
// error stays in the chain, so errors.As still reaches *pgconn.PgError.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case transient(err):
		return fmt.Errorf("%w: %s: %w", ports.ErrUnavailable, op, err)
	case dataException(err):
		return fmt.Errorf("%w: %s: %w", ports.ErrDataException, op, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}
