//! libp2p transport backed by [iroh](https://iroh.computer)'s QUIC stack.
//!
//! This crate is vendored and adapted from the upstream `libp2p-iroh` crate
//! (<https://github.com/rustonbsd/libp2p-iroh>, MIT licensed, by Zacharias Boehler).
//! It targets the same `libp2p` 0.56 used by this workspace; the only change from
//! upstream is bumping `iroh` 0.97 -> 1.0 (0.97 pins a pre-release `sha2` that
//! conflicts with `litep2p`'s released `sha2 0.11`). It is experimental and intended
//! for evaluating iroh as an alternative transport (NAT traversal, relay fallback,
//! hole punching) for the libp2p network backend.
//!
//! The transport directly yields `(PeerId, StreamMuxerBox)` because iroh's QUIC
//! connections are already authenticated and multiplexed; it therefore bypasses the
//! noise + yamux upgrade pipeline and is combined with the existing TCP/WS transport
//! via `OrTransport`, the same way `libp2p-quic` is integrated upstream.

mod connection;
mod helper;
mod stream;
mod transport;

pub use connection::{Connecting, Connection, ConnectionError, ConnectionErrorKind};
pub use helper::*;
pub use stream::{Stream, StreamError, StreamErrorKind};
pub use transport::{Transport, TransportError, TransportErrorKind};

pub use libp2p::Transport as TransportTrait;
