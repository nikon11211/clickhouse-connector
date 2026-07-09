package clickhouse_connector

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs []attribute.KeyValue) (context.Context, interface{})
	EndSpan(span interface{}, err error, startTime time.Time)
}

type OpenTelemetryTracer struct {
	tracer trace.Tracer
}

func NewOpenTelemetryTracer(tracer trace.Tracer) *OpenTelemetryTracer {
	return &OpenTelemetryTracer{tracer: tracer}
}

func (t *OpenTelemetryTracer) StartSpan(ctx context.Context, name string, attrs []attribute.KeyValue) (context.Context, interface{}) {
	ctx, span := t.tracer.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
	return ctx, span
}

func (t *OpenTelemetryTracer) EndSpan(span interface{}, err error, startTime time.Time) {
	otelSpan, ok := span.(trace.Span)
	if !ok {
		return
	}
	defer otelSpan.End()

	duration := time.Since(startTime)

	if err != nil {
		otelSpan.RecordError(err)
		otelSpan.SetStatus(codes.Error, err.Error())
		otelSpan.SetAttributes(
			attribute.String("error.type", classifyError(err)),
			attribute.Bool("error", true),
		)
	} else {
		otelSpan.SetStatus(codes.Ok, "success")
		otelSpan.SetAttributes(
			attribute.Bool("success", true),
		)
	}

	otelSpan.SetAttributes(
		attribute.Float64("duration_ms", float64(duration.Microseconds())/1000),
	)
}

type NoOpTracer struct{}

func (t *NoOpTracer) StartSpan(ctx context.Context, name string, attrs []attribute.KeyValue) (context.Context, interface{}) {
	return ctx, nil
}

func (t *NoOpTracer) EndSpan(span interface{}, err error, startTime time.Time) {}
