package interceptor

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestKeyFromMetadataFound(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "alice"))
	key, ok := KeyFromMetadata("x-api-key")(ctx)
	if !ok || key != "alice" {
		t.Fatalf("key = %q, ok = %v, want alice, true", key, ok)
	}
}

func TestKeyFromMetadataMissingHeader(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	if _, ok := KeyFromMetadata("x-api-key")(ctx); ok {
		t.Fatal("want ok = false when the header is absent")
	}
}

func TestKeyFromMetadataNoIncomingMetadata(t *testing.T) {
	if _, ok := KeyFromMetadata("x-api-key")(context.Background()); ok {
		t.Fatal("want ok = false when there is no incoming metadata at all")
	}
}

func TestKeyFromMetadataEmptyValue(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", ""))
	if _, ok := KeyFromMetadata("x-api-key")(ctx); ok {
		t.Fatal("want ok = false for an empty header value")
	}
}

func TestKeyFromMetadataFirstOfMultipleValues(t *testing.T) {
	md := metadata.MD{}
	md.Append("x-api-key", "first")
	md.Append("x-api-key", "second")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	key, ok := KeyFromMetadata("x-api-key")(ctx)
	if !ok || key != "first" {
		t.Fatalf("key = %q, ok = %v, want first, true", key, ok)
	}
}

func TestKeyFromMetadataIsCaseInsensitive(t *testing.T) {
	// gRPC lowercases metadata keys; metadata.Pairs/Get do the same, so
	// a header set as X-Api-Key is still found under x-api-key.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("X-Api-Key", "alice"))
	key, ok := KeyFromMetadata("x-api-key")(ctx)
	if !ok || key != "alice" {
		t.Fatalf("key = %q, ok = %v, want alice, true", key, ok)
	}
}
