package taskiq

import (
	"context"
	"io"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0" // Use appropriate semantic conventions
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "taskiq-worker"
)

// Tracer is a global tracer instance.
var Tracer trace.Tracer

// InitTracerProvider initializes an OpenTelemetry tracer provider and sets the global Tracer.
// It uses a stdout exporter for now.
func InitTracerProvider() (*sdktrace.TracerProvider, error) {
	// For demo purposes, we'll export to stdout.
	// In a production environment, you'd use a more robust exporter (e.g., Jaeger, Zipkin, OTLP).
	exporter, err := stdouttrace.New(
		stdouttrace.WithWriter(os.Stdout), // You can use os.Stderr or a file as well
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithoutTimestamps(), // Optional: if you don't want timestamps in stdout
	)
	if err != nil {
		return nil, err
	}

	// Define the service name for your application.
	// It's good practice to use semantic conventions for resource attributes.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("taskiq-worker-service"),
			semconv.ServiceVersionKey.String("v0.1.0"),
		),
	)
	if err != nil {
		return nil, err
	}

	// Create a new tracer provider with the exporter and resource.
	// We're using AlwaysSample sampler for demo purposes to ensure all traces are captured.
	// In production, you'd likely use ParentBased(AlwaysSample()) or TraceIDRatioBased().
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	// Set the global tracer provider.
	otel.SetTracerProvider(tp)

	// Set the global Tracer instance.
	Tracer = otel.Tracer(tracerName)

	Logger.Info().Msg("OpenTelemetry TracerProvider initialized with stdout exporter.")
	return tp, nil
}

// InitNoOpTracerProvider initializes a no-op tracer provider for tests or when tracing is disabled.
func InitNoOpTracerProvider() {
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	Tracer = otel.Tracer(tracerName) // This will be a no-op tracer
	Logger.Info().Msg("OpenTelemetry NoOp TracerProvider initialized.")
}

// NewSpan creates a new span from the global Tracer.
func NewSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if Tracer == nil {
		// This might happen if InitTracerProvider wasn't called or failed.
		// Fallback to a NoopTracer to prevent panics, though ideally, Tracer should always be initialized.
		return trace.NewNoopTracerProvider().Tracer(tracerName).Start(ctx, name, opts...)
	}
	return Tracer.Start(ctx, name, opts...)
}

// SpanFromContext returns the span from the current context, if any.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}
