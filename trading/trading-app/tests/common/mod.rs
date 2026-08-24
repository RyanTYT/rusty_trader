use std::sync::OnceLock;

use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

static TRACING_INIT: OnceLock<()> = OnceLock::new();

pub fn init_tracing() {
    TRACING_INIT.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("trading_app=info,warn"));

        let layer = fmt::layer()
            .pretty()
            .with_target(true)
            .with_line_number(true)
            .with_filter(filter);

        tracing_subscriber::registry().with(layer).init();
    });
}
