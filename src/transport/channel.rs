use bytes::Bytes;
use std::{
  cell::RefCell,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll},
};

pub struct H2RespBody {
  recv: h2::RecvStream,
}
impl http_body::Body for H2RespBody {
  type Data = Bytes;
  type Error = tonic::Status;
  fn poll_frame(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
    match Pin::new(&mut self.recv).poll_data(cx) {
      Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(http_body::Frame::data(bytes)))),
      Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(tonic::Status::unknown(e.to_string())))),
      Poll::Ready(None) => match Pin::new(&mut self.recv).poll_trailers(cx) {
        Poll::Ready(Ok(Some(tr))) => Poll::Ready(Some(Ok(http_body::Frame::trailers(tr)))),
        Poll::Ready(Ok(None)) => Poll::Ready(None),
        Poll::Ready(Err(e)) => Poll::Ready(Some(Err(tonic::Status::unknown(e.to_string())))),
        Poll::Pending => Poll::Pending,
      },
      Poll::Pending => Poll::Pending,
    }
  }
}

/// Custom tonic Channel implementation
///
/// The reason we don't use tonic's built-in channel is that its implementation is tightly coupled with tokio and tower.
/// Specifically, it uses `tokio::spawn` to start tasks, which is hard-coded and cannot be replaced with my custom executor.
/// This library does not allow a Rust-side IO event loop, so we cannot use it.
#[derive(Clone)]
pub struct Channel {
  sender: Rc<RefCell<Option<h2::client::SendRequest<Bytes>>>>,
  authority: String,
  scheme: String,
  reactor: crate::runtime::Reactor,
}
impl Channel {
  /// Create a new Channel with custom scheme
  pub fn new(
    sender: Rc<RefCell<Option<h2::client::SendRequest<Bytes>>>>,
    authority: String,
    scheme: String,
    reactor: crate::runtime::Reactor,
  ) -> Self {
    Self {
      sender,
      authority,
      scheme,
      reactor,
    }
  }

  /// Ensure URI has complete scheme and authority
  fn ensure_complete_uri(
    uri: http::Uri,
    scheme: &str,
    authority: &str,
  ) -> Result<http::Uri, tonic::Status> {
    // If URI already has scheme and authority, use as-is
    if uri.scheme().is_some() && uri.authority().is_some() {
      return Ok(uri);
    }

    // Otherwise, build complete URI with provided scheme and authority
    let path_and_query = uri
      .path_and_query()
      .cloned()
      .unwrap_or_else(|| http::uri::PathAndQuery::from_static("/"));

    http::Uri::builder()
      .scheme(scheme)
      .authority(authority)
      .path_and_query(path_and_query)
      .build()
      .map_err(|e| tonic::Status::internal(format!("Invalid URI: {}", e)))
  }

  /// Prepare HTTP/2 request from hyper request
  fn prepare_h2_request(
    &self,
    req: hyper::Request<tonic::body::Body>,
  ) -> Result<(http::Request<()>, tonic::body::Body), tonic::Status> {
    let (mut parts, body) = req.into_parts();

    // Ensure URI has complete scheme and authority for HTTP/2
    parts.uri = Self::ensure_complete_uri(parts.uri, &self.scheme, &self.authority)?;

    // Build HTTP/2 request
    let mut h2_req = http::Request::builder()
      .method(parts.method)
      .uri(parts.uri)
      .version(http::Version::HTTP_2)
      .body(())
      .map_err(|e| tonic::Status::internal(format!("Failed to build request: {}", e)))?;
    *h2_req.headers_mut() = parts.headers;

    Ok((h2_req, body))
  }

  /// Send resquest body
  fn send_body(
    reactor: crate::runtime::Reactor,
    mut body: tonic::body::Body,
    mut send_stream: h2::SendStream<Bytes>,
  ) {
    use http_body_util::BodyExt;

    reactor.spawn(async move {
      let mut ended = false;

      while let Some(frame_res) = body.frame().await {
        let frame = match frame_res.map_err(|e| tonic::Status::from_error(Box::new(e))) {
          Ok(f) => f,
          Err(_e) => break,
        };

        match frame.into_data() {
          Ok(data) => {
            if Self::send_data_chunk(&mut send_stream, data).await.is_err() {
              break;
            }
          }
          Err(f) => {
            if let Ok(trailers) = f.into_trailers() {
              let _ = send_stream.send_trailers(trailers);
              ended = true;
            }
          }
        }
      }

      if !ended {
        let _ = send_stream.send_data(Bytes::new(), true);
      }
    });
  }

  /// Send a data chunk through the HTTP/2 stream
  async fn send_data_chunk(
    send_stream: &mut h2::SendStream<Bytes>,
    mut data: Bytes,
  ) -> Result<(), ()> {
    log::trace!(
      "Channel: got request body frame data of {} bytes",
      data.len()
    );

    while !data.is_empty() {
      send_stream.reserve_capacity(data.len());

      match std::future::poll_fn(|cx| send_stream.poll_capacity(cx)).await {
        Some(Ok(cap)) => {
          if cap == 0 {
            continue;
          }
          let n = std::cmp::min(cap, data.len());
          let chunk = data.split_to(n);
          log::trace!("Channel: send_data {} bytes (cap={})", n, cap);
          let _ = send_stream.send_data(chunk, false);
        }
        Some(Err(_)) | None => {
          return Err(());
        }
      }
    }
    Ok(())
  }
}

impl tower::Service<hyper::Request<tonic::body::Body>> for Channel {
  type Response = http::Response<H2RespBody>;
  type Error = tonic::Status;
  type Future =
    Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + 'static>>;

  /// Checke whether the client is ready
  /// Especially whether HTTP/2 has already completed the handshake
  fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    if self.sender.borrow().is_some() {
      Poll::Ready(Ok(()))
    } else {
      Poll::Pending
    }
  }

  /// Call gRPC server
  fn call(&mut self, req: hyper::Request<tonic::body::Body>) -> Self::Future {
    let sender_opt = self.sender.borrow().clone();
    let reactor = self.reactor.clone();
    let channel = self.clone();

    Box::pin(async move {
      let mut sender = sender_opt.ok_or_else(|| tonic::Status::unavailable("h2 sender missing"))?;

      // Prepare HTTP/2 request
      let (h2_req, body) = channel.prepare_h2_request(req)?;

      // Send request and get response future + send stream
      let (resp_fut, send_stream) = sender
        .send_request(h2_req, false)
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;

      // Spawn background task to handle request body streaming
      Self::send_body(reactor, body, send_stream);

      // Wait for response
      let resp = resp_fut
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;

      Ok(resp.map(|rs| H2RespBody { recv: rs }))
    })
  }
}
