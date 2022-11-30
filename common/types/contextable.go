package types

import "context"

type Contextable struct {
	ctx context.Context
}

// Context returns the context
func (r *Contextable) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// SetContext sets the context
func (r *Contextable) SetContext(ctx context.Context) {
	r.ctx = ctx
}
