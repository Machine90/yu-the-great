// pub(crate) mod rpc_post_office;
#[cfg(feature = "multi")] pub mod multi;
#[cfg(feature = "single")] pub mod single;

pub struct StubConfig {
    pub address: String, 
    pub print_banner: bool, 
    pub run_as_daemon: bool
}
