// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! Transport that serves as a common ground for all connections.

use either::Either;
use futures::{
	future::{MapOk, TryFutureExt},
	io::{IoSlice, IoSliceMut},
	ready, AsyncRead, AsyncWrite,
};
use libp2p::{
	core::{
		muxing::{StreamMuxer, StreamMuxerBox, StreamMuxerEvent},
		transport::{
			Boxed, DialOpts, ListenerId, OptionalTransport, TransportError, TransportEvent,
		},
		upgrade,
	},
	dns, identity, noise, tcp, websocket, PeerId, Transport,
};
use std::{
	convert::TryFrom as _,
	io,
	pin::Pin,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
	task::{Context, Poll},
	time::Duration,
};

/// Allows querying the total bandwidth produced by the transport.
#[derive(Default)]
pub struct BandwidthSinks {
	inbound: AtomicU64,
	outbound: AtomicU64,
}

impl BandwidthSinks {
	/// Returns the total number of bytes downloaded on all streams.
	pub fn total_inbound(&self) -> u64 {
		self.inbound.load(Ordering::Relaxed)
	}

	/// Returns the total number of bytes uploaded on all streams.
	pub fn total_outbound(&self) -> u64 {
		self.outbound.load(Ordering::Relaxed)
	}
}

/// Builds the transport that serves as a common ground for all connections.
///
/// If `memory_only` is true, then only communication within the same process are allowed. Only
/// addresses with the format `/memory/...` are allowed.
///
/// Returns a `BandwidthSinks` object that allows querying the total bandwidth produced by all
/// the connections spawned with this transport.
pub fn build_transport(
	keypair: identity::Keypair,
	memory_only: bool,
) -> (Boxed<(PeerId, StreamMuxerBox)>, Arc<BandwidthSinks>) {
	// Build the base layer of the transport.
	let transport = if !memory_only {
		// Main transport: DNS(TCP)
		let tcp_config = tcp::Config::new().nodelay(true);
		let tcp_trans = tcp::tokio::Transport::new(tcp_config.clone());
		let dns_init = dns::tokio::Transport::system(tcp_trans);

		Either::Left(if let Ok(dns) = dns_init {
			// WS + WSS transport
			//
			// Main transport can't be used for `/wss` addresses because WSS transport needs
			// unresolved addresses (BUT WSS transport itself needs an instance of DNS transport to
			// resolve and dial addresses).
			let tcp_trans = tcp::tokio::Transport::new(tcp_config);
			let dns_for_wss = dns::tokio::Transport::system(tcp_trans)
				.expect("same system_conf & resolver to work");
			Either::Left(websocket::Config::new(dns_for_wss).or_transport(dns))
		} else {
			// In case DNS can't be constructed, fallback to TCP + WS (WSS won't work)
			let tcp_trans = tcp::tokio::Transport::new(tcp_config.clone());
			let desktop_trans = websocket::Config::new(tcp_trans)
				.or_transport(tcp::tokio::Transport::new(tcp_config));
			Either::Right(desktop_trans)
		})
	} else {
		Either::Right(OptionalTransport::some(libp2p::core::transport::MemoryTransport::default()))
	};

	let authentication_config = noise::Config::new(&keypair).expect("Can create noise config. qed");
	let multiplexing_config = libp2p::yamux::Config::default();

	let transport = transport
		.upgrade(upgrade::Version::V1Lazy)
		.authenticate(authentication_config)
		.multiplex(multiplexing_config)
		.timeout(Duration::from_secs(20));

	let bandwidth = Arc::new(BandwidthSinks::default());
	let mut registry = libp2p::metrics::Registry::default();

	// rust-libp2p 0.56 removed `Transport::with_bandwidth_logging`; the recommended replacement
	// is the metrics transport from `libp2p-metrics`.
	let transport = libp2p::metrics::BandwidthTransport::new(transport, &mut registry);
	let transport = CountingTransport::new(transport, Arc::clone(&bandwidth))
		.map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
		.boxed();

	(transport, bandwidth)
}

#[derive(Clone)]
#[pin_project::pin_project]
struct CountingTransport<T> {
	#[pin]
	transport: T,
	bandwidth: Arc<BandwidthSinks>,
}

impl<T> CountingTransport<T> {
	fn new(transport: T, bandwidth: Arc<BandwidthSinks>) -> Self {
		Self { transport, bandwidth }
	}
}

impl<T, M> Transport for CountingTransport<T>
where
	T: Transport<Output = (PeerId, M)>,
	M: StreamMuxer + Send + 'static,
	M::Substream: Send + 'static,
	M::Error: Send + Sync + 'static,
{
	type Output = (PeerId, CountingMuxer<M>);
	type Error = T::Error;
	type ListenerUpgrade = MapOk<
		T::ListenerUpgrade,
		Box<dyn FnOnce((PeerId, M)) -> (PeerId, CountingMuxer<M>) + Send>,
	>;
	type Dial = MapOk<T::Dial, Box<dyn FnOnce((PeerId, M)) -> (PeerId, CountingMuxer<M>) + Send>>;

	fn listen_on(
		&mut self,
		id: ListenerId,
		addr: libp2p::Multiaddr,
	) -> Result<(), TransportError<Self::Error>> {
		self.transport.listen_on(id, addr)
	}

	fn remove_listener(&mut self, id: ListenerId) -> bool {
		self.transport.remove_listener(id)
	}

	fn dial(
		&mut self,
		addr: libp2p::Multiaddr,
		dial_opts: DialOpts,
	) -> Result<Self::Dial, TransportError<Self::Error>> {
		let bandwidth = Arc::clone(&self.bandwidth);
		Ok(self.transport.dial(addr, dial_opts)?.map_ok(Box::new(move |(peer_id, muxer)| {
			(peer_id, CountingMuxer::new(muxer, bandwidth))
		})))
	}

	fn poll(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
		let this = self.project();

		match this.transport.poll(cx) {
			Poll::Ready(TransportEvent::Incoming {
				listener_id,
				upgrade,
				local_addr,
				send_back_addr,
			}) => {
				let bandwidth = Arc::clone(this.bandwidth);
				Poll::Ready(TransportEvent::Incoming {
					listener_id,
					upgrade: upgrade.map_ok(Box::new(move |(peer_id, muxer)| {
						(peer_id, CountingMuxer::new(muxer, bandwidth))
					})),
					local_addr,
					send_back_addr,
				})
			},
			Poll::Ready(other) => {
				let mapped = other.map_upgrade(|_upgrade| unreachable!("case already matched"));
				Poll::Ready(mapped)
			},
			Poll::Pending => Poll::Pending,
		}
	}
}

#[derive(Clone)]
#[pin_project::pin_project]
struct CountingMuxer<M> {
	#[pin]
	inner: M,
	bandwidth: Arc<BandwidthSinks>,
}

impl<M> CountingMuxer<M> {
	fn new(inner: M, bandwidth: Arc<BandwidthSinks>) -> Self {
		Self { inner, bandwidth }
	}
}

impl<M> StreamMuxer for CountingMuxer<M>
where
	M: StreamMuxer,
{
	type Substream = CountingStream<M::Substream>;
	type Error = M::Error;

	fn poll(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Result<StreamMuxerEvent, Self::Error>> {
		let this = self.project();
		this.inner.poll(cx)
	}

	fn poll_inbound(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Result<Self::Substream, Self::Error>> {
		let this = self.project();
		let inner = ready!(this.inner.poll_inbound(cx)?);
		Poll::Ready(Ok(CountingStream::new(inner, Arc::clone(this.bandwidth))))
	}

	fn poll_outbound(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Result<Self::Substream, Self::Error>> {
		let this = self.project();
		let inner = ready!(this.inner.poll_outbound(cx)?);
		Poll::Ready(Ok(CountingStream::new(inner, Arc::clone(this.bandwidth))))
	}

	fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		let this = self.project();
		this.inner.poll_close(cx)
	}
}

#[pin_project::pin_project]
struct CountingStream<S> {
	#[pin]
	inner: S,
	bandwidth: Arc<BandwidthSinks>,
}

impl<S> CountingStream<S> {
	fn new(inner: S, bandwidth: Arc<BandwidthSinks>) -> Self {
		Self { inner, bandwidth }
	}
}

impl<S: AsyncRead> AsyncRead for CountingStream<S> {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		let this = self.project();
		let bytes = ready!(this.inner.poll_read(cx, buf))?;
		this.bandwidth
			.inbound
			.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
		Poll::Ready(Ok(bytes))
	}

	fn poll_read_vectored(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &mut [IoSliceMut<'_>],
	) -> Poll<io::Result<usize>> {
		let this = self.project();
		let bytes = ready!(this.inner.poll_read_vectored(cx, bufs))?;
		this.bandwidth
			.inbound
			.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
		Poll::Ready(Ok(bytes))
	}
}

impl<S: AsyncWrite> AsyncWrite for CountingStream<S> {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		let this = self.project();
		let bytes = ready!(this.inner.poll_write(cx, buf))?;
		this.bandwidth
			.outbound
			.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
		Poll::Ready(Ok(bytes))
	}

	fn poll_write_vectored(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &[IoSlice<'_>],
	) -> Poll<io::Result<usize>> {
		let this = self.project();
		let bytes = ready!(this.inner.poll_write_vectored(cx, bufs))?;
		this.bandwidth
			.outbound
			.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
		Poll::Ready(Ok(bytes))
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let this = self.project();
		this.inner.poll_flush(cx)
	}

	fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let this = self.project();
		this.inner.poll_close(cx)
	}
}
