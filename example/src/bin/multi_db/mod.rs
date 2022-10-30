#[allow(unused)]
pub(super) mod kv;
pub mod node_three;
// pub mod mock_federation;

use components::vendor::prelude::{
    conf_file_logger, init_logger_factory, LogFactory, LogLevel, LoggerConfig,
};

pub fn set_logger(level: LogLevel, file_log: bool) {
    // create a default factory
    let mut factory = LogFactory::default();
    let logger = if file_log {
        conf_file_logger("./multi_raft.log", LoggerConfig::in_level(level))
    } else {
        LoggerConfig::in_level(level).into()
    };
    // can change default logger that macros used
    factory.change_default_logger(logger);
    // then set it before use.
    init_logger_factory(factory);
}
