//! Thin wrapper over `tungstenite`'s synchronous WebSocket client with
//! `rustls`. Mirrors the C++ `ws_client.h`: timestamps each message as soon
//! as it is received, tracks PING/PONG round trips.
//!
//! tungstenite reassembles fragmented messages and auto-replies to the
//! server's PINGs, so none of that lives here.

use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Bytes, Message, WebSocket};

use crate::time::wall_us;

pub struct WsClient {
    sock: WebSocket<MaybeTlsStream<TcpStream>>,
    url: String,
    pending_ping_send_us: i64,
    rtts_us: Vec<i64>,
}

/// Host and port from "wss://host[:port]/path" (443 default for wss).
fn host_port(url: &str) -> Result<(String, u16), String> {
    let rest = url
        .strip_prefix("wss://")
        .or_else(|| url.strip_prefix("ws://"))
        .ok_or_else(|| format!("unsupported url scheme: {url}"))?;
    let default_port = if url.starts_with("wss://") { 443 } else { 80 };
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    match authority.rsplit_once(':') {
        Some((h, p)) => Ok((h.to_string(), p.parse().map_err(|_| format!("bad port in {url}"))?)),
        None => Ok((authority.to_string(), default_port)),
    }
}

impl WsClient {
    /// TCP connect with timeout, TLS + WebSocket handshake.
    pub fn connect(url: &str, connect_timeout: Duration) -> Result<WsClient, String> {
        let (host, port) = host_port(url)?;
        let addr = (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| format!("resolve {host}: {e}"))?
            .next()
            .ok_or_else(|| format!("resolve {host}: no address"))?;
        let tcp = TcpStream::connect_timeout(&addr, connect_timeout).map_err(|e| format!("connect {addr}: {e}"))?;
        tcp.set_nodelay(true).ok();
        // Bound the TLS + HTTP upgrade handshake too.
        tcp.set_read_timeout(Some(connect_timeout)).ok();
        tcp.set_write_timeout(Some(connect_timeout)).ok();
        let (sock, _resp) = tungstenite::client_tls(url, tcp).map_err(|e| format!("websocket handshake failed: {e}"))?;
        let mut c = WsClient { sock, url: url.to_string(), pending_ping_send_us: 0, rtts_us: Vec::new() };
        c.with_tcp(|t| {
            t.set_write_timeout(None).ok();
        });
        Ok(c)
    }

    fn with_tcp<F: FnOnce(&TcpStream)>(&mut self, f: F) {
        match self.sock.get_mut() {
            MaybeTlsStream::Plain(t) => f(t),
            MaybeTlsStream::Rustls(s) => f(s.get_ref()),
            _ => {}
        }
    }

    /// Blocking mode: `recv()` waits up to `timeout` for a message.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.with_tcp(|t| {
            t.set_nonblocking(false).ok();
            t.set_read_timeout(timeout).ok();
        });
    }

    /// Busy-poll mode: `recv()` returns immediately with `None` if nothing is pending.
    pub fn set_nonblocking(&mut self, on: bool) {
        self.with_tcp(|t| {
            t.set_nonblocking(on).ok();
        });
    }

    /// One complete text/binary message, timestamped right after it is read.
    /// `Ok(None)` on timeout / nothing pending.
    pub fn recv(&mut self) -> Result<Option<(Bytes, i64)>, String> {
        loop {
            match self.sock.read() {
                Ok(Message::Text(t)) => {
                    let ts = wall_us();
                    return Ok(Some((Bytes::from(t), ts)));
                }
                Ok(Message::Binary(b)) => {
                    let ts = wall_us();
                    return Ok(Some((b, ts)));
                }
                Ok(Message::Pong(_)) => {
                    if self.pending_ping_send_us != 0 {
                        self.rtts_us.push(wall_us() - self.pending_ping_send_us);
                        self.pending_ping_send_us = 0;
                    }
                }
                Ok(Message::Ping(_)) | Ok(Message::Frame(_)) => {} // PONG is queued by tungstenite
                Ok(Message::Close(_)) => return Err("server sent CLOSE frame".into()),
                Err(tungstenite::Error::Io(e))
                    if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    return Ok(None)
                }
                Err(e) => return Err(format!("websocket read: {e}")),
            }
        }
    }

    /// Sends a WS PING; `recv()` records the RTT when the PONG arrives. Only
    /// one ping outstanding at a time.
    pub fn send_ping(&mut self) -> Result<(), String> {
        self.sock
            .send(Message::Ping(Bytes::from_static(b"l2")))
            .map_err(|e| format!("websocket send PING: {e}"))?;
        self.pending_ping_send_us = wall_us();
        Ok(())
    }

    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn take_rtts(&mut self) -> Vec<i64> {
        std::mem::take(&mut self.rtts_us)
    }
}
