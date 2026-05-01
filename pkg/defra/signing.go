package defra

import (
	"context"

	acpIdentity "github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/immutable"
)

type identityContextKey struct{}

func WithContext(ctx context.Context, identity immutable.Option[acpIdentity.Identity]) context.Context {
	if identity.HasValue() {
		return context.WithValue(ctx, identityContextKey{}, identity.Value())
	}
	return context.WithValue(ctx, identityContextKey{}, nil)
}

func FromContext(ctx context.Context) immutable.Option[acpIdentity.Identity] {
	ident, ok := ctx.Value(identityContextKey{}).(acpIdentity.Identity)
	if ok {
		return immutable.Some(ident)
	}
	return acpIdentity.None
}
