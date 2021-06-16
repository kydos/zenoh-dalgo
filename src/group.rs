use zenoh::net::protocol::core::PeerId;

enum ZGroupEvent {
    Join { mid: PeerId, period: std::time::Duration, liveliness: std::time::Duration},
    Leave(PeerId),
    KeepAlive(PeerId, std::time::Instant)
}
