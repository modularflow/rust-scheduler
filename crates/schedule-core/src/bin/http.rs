#[cfg(feature = "http_api")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::SocketAddr;

    use schedule_tool::{Schedule, http_api};

    let addr: SocketAddr = std::env::var("SCHEDULE_TOOL_HTTP_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_string())
        .parse()?;

    println!("schedule-tool HTTP API listening on http://{addr}");
    let schedule = Schedule::new();
    http_api::serve(addr, schedule).await?;
    Ok(())
}

#[cfg(not(feature = "http_api"))]
fn main() {
    eprintln!("Rebuild with the `http_api` feature to enable the HTTP server.");
}
