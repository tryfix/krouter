package krouter

import "context"

type Validator interface {
	Validate(ctx context.Context, v interface{}, params map[string]string) error
}
