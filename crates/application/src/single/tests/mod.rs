#[cfg(all(test, feature = "rpc_transport"))]
mod functions;

use common::vendor::prelude::{conf_file_logger, init_logger_factory, LoggerConfig, LogLevel, LogFactory};

pub(self) fn set_logger(module: &str) {
    let logger = conf_file_logger(
        format!("./single_{module}.log"),
        LoggerConfig::in_level(LogLevel::Trace),
    );
    init_logger_factory(LogFactory::default().use_logger(logger));
}
