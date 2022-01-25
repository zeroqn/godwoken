use anyhow::Result;
use gw_config::Trace;
use tracing_subscriber::prelude::*;

pub struct ShutdownGuard {
    trace: Trace,
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if matches!(self.trace, Trace::Jaeger) {
            opentelemetry::global::shutdown_tracer_provider(); // Sending remaining spans
        }
    }
}

pub fn init(trace: Trace) -> Result<ShutdownGuard> {
    let env_filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))?;

    let registry = tracing_subscriber::registry().with(env_filter_layer);
    match trace {
        Trace::Jaeger => {
            opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

            let jaeger_layer = {
                let tracer = opentelemetry_jaeger::new_pipeline()
                    .with_service_name("godwoken")
                    .install_batch(opentelemetry::runtime::Tokio)?;
                tracing_opentelemetry::layer().with_tracer(tracer)
            };

            registry.with(jaeger_layer).try_init()?;
        }
    }

    Ok(ShutdownGuard { trace })
}
