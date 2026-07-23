package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/yasyf/daemonkit/daemon"
)

// DefaultObservationMaxResponse bounds an observation response when the route
// does not select a smaller limit.
const DefaultObservationMaxResponse = 64 << 10

// ObservationRequest is the immutable authenticated view available to a
// read-only observation handler.
type ObservationRequest struct {
	Op          Op
	Tenant      string
	Peer        Peer
	WireBuild   string
	Payload     []byte
	Publication daemon.Publication
}

// ObservationResponse is one bounded unary JSON response.
type ObservationResponse struct {
	Payload json.RawMessage
}

// ObservationHandler answers a read-only product observation without access
// to the accepted session, streaming, event, or mutation capabilities.
type ObservationHandler func(context.Context, ObservationRequest) (ObservationResponse, error)

// ObservationRoute declares one suite-qualified immutable observation op.
type ObservationRoute struct {
	Op               Op
	MaxResponseBytes int
	Handler          ObservationHandler
}

func observationHandlers(routes []ObservationRoute, maxFrame int) (map[Op]Handler, error) {
	handlers := make(map[Op]Handler, len(routes))
	for _, route := range routes {
		if err := validateObservationRoute(route, maxFrame); err != nil {
			return nil, err
		}
		if _, exists := handlers[route.Op]; exists {
			return nil, fmt.Errorf("wire: observation op %q is duplicated", route.Op)
		}
		maxResponse := route.MaxResponseBytes
		if maxResponse == 0 {
			maxResponse = DefaultObservationMaxResponse
		}
		handler := route.Handler
		handlers[route.Op] = func(ctx context.Context, req Request) (any, error) {
			response, err := handler(ctx, ObservationRequest{
				Op: req.Op, Tenant: req.Tenant, Peer: req.Peer, WireBuild: req.WireBuild,
				Payload:     append([]byte(nil), req.Payload...),
				Publication: req.Publication,
			})
			if err != nil {
				return nil, err
			}
			if len(response.Payload) > maxResponse {
				return nil, fmt.Errorf("wire: observation response is %d bytes; limit is %d", len(response.Payload), maxResponse)
			}
			if !json.Valid(response.Payload) {
				return nil, errors.New("wire: observation response is not valid JSON")
			}
			return response.Payload, nil
		}
	}
	return handlers, nil
}

func validateObservationRoute(route ObservationRoute, maxFrame int) error {
	op := string(route.Op)
	if route.Handler == nil {
		return fmt.Errorf("wire: observation op %q requires a handler", route.Op)
	}
	if strings.HasPrefix(op, "daemon.") {
		return fmt.Errorf("wire: observation op %q uses daemonkit's private namespace", route.Op)
	}
	if strings.Count(op, ".") < 2 || strings.HasPrefix(op, ".") || strings.HasSuffix(op, ".") {
		return fmt.Errorf("wire: observation op %q must be suite-qualified", route.Op)
	}
	limit := route.MaxResponseBytes
	if limit < 0 {
		return fmt.Errorf("wire: observation op %q response limit must not be negative", route.Op)
	}
	if limit == 0 {
		limit = DefaultObservationMaxResponse
	}
	if limit > maxFrame {
		return fmt.Errorf("wire: observation op %q response limit %d exceeds frame limit %d", route.Op, limit, maxFrame)
	}
	return nil
}
