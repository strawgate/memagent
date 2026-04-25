pub fn loopback_client() -> ureq::Agent {
    ureq::Agent::config_builder()
        .proxy(None)
        .timeout_global(Some(std::time::Duration::from_secs(5)))
        .build()
        .into()
}
