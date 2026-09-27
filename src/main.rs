fn main() {
    println!("Raft");
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("Starting RustyRaft");
}
