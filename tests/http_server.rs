use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn wait_for_http_server(address: SocketAddr) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);

    while Instant::now() < deadline {
        if let Ok(mut stream) = TcpStream::connect(address) {
            let _ = stream.set_read_timeout(
                Some(Duration::from_millis(500)),
            );

            let request = format!(
                "GET /leader HTTP/1.1\r\n\
                 Host: {}\r\n\
                 Connection: close\r\n\r\n",
                address,
            );

            if stream.write_all(request.as_bytes()).is_ok() {
                let mut response = String::new();

                if stream.read_to_string(&mut response).is_ok()
                    && response.starts_with("HTTP/1.1 200")
                {
                    return true;
                }
            }
        }

        thread::sleep(Duration::from_millis(50));
    }

    false
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn http_server_starts_and_serves_leader_endpoint() {
    // NodeConfig uses HTTP_PORT_BASE (3000) + node ID.
    let address: SocketAddr = "127.0.0.1:3001"
        .parse()
        .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_rustyraft"))
        .args(["1", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("should start rustyraft");

    let started = wait_for_http_server(address);

    stop_child(&mut child);

    assert!(
        started,
        "HTTP server should serve /leader at {}",
        address,
    );
}