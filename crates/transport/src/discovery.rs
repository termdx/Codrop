//! LAN peer discovery over mDNS (`mdns-sd`).
//!
//! Nodes advertise `_codrop._tcp.local.` and browse for the same. This realizes the
//! "P2P-LAN-first" topology: zero-config discovery on the local network, no central server.
//! (mDNS does not traverse loopback-only setups; on a real LAN interface it works.)

use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::time::{Duration, Instant};

const SERVICE: &str = "_codrop._tcp.local.";

/// Advertise this node. The returned daemon must be kept alive to stay registered.
pub fn advertise(instance: &str, port: u16) -> Result<ServiceDaemon> {
    let daemon = ServiceDaemon::new()?;
    let host = format!("{instance}.local.");
    // Empty IP + `enable_addr_auto` lets mdns-sd fill in this host's addresses.
    let info = ServiceInfo::new(SERVICE, instance, &host, "", port, &[("role", "codrop")][..])?
        .enable_addr_auto();
    daemon.register(info)?;
    Ok(daemon)
}

/// Browse for peers for `timeout`, printing each resolved one.
pub fn discover(timeout: Duration) -> Result<()> {
    let daemon = ServiceDaemon::new()?;
    let receiver = daemon.browse(SERVICE)?;
    println!("browsing {SERVICE} for {timeout:?} ...");

    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match receiver.recv_timeout(Duration::from_millis(500)) {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                println!(
                    "peer: {} addrs={:?} port={}",
                    info.get_fullname(),
                    info.get_addresses(),
                    info.get_port()
                );
            }
            Ok(_) => {}             // searching/registered noise
            Err(_) => continue,     // recv timeout — keep waiting until the deadline
        }
    }
    Ok(())
}
