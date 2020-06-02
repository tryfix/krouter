package krouter

import (
	"context"
	"net/http"
)

type ErrorHandler interface {
	Handle(w http.ResponseWriter, err error) error
}

type SuccessHandler interface {
	Handle(w http.ResponseWriter, data interface{}) error
}

type SuccessHandlerFunc func(ctx context.Context, w http.ResponseWriter, r *http.Request, payload HttpPayload) error
type ErrorHandlerFunc func(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) error
