#[tokio::main]
async fn main() -> Result<(), wakemc::error::ProxyError> {
    wakemc::run().await
}
