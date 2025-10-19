use std::{cell::RefCell, collections::HashMap, rc::Rc, task::Waker};

use bytes::Bytes;
use http::uri::{InvalidUri, PathAndQuery};
use protobuf::{
  MessageDyn,
  reflect::{FileDescriptor, MessageDescriptor},
};
use std::str::FromStr;
use tokio::sync::mpsc::{Receiver, Sender, channel, error::TrySendError};
use tokio_stream::wrappers::ReceiverStream;

use crate::{runtime, transport::channel::Channel};

/// Represents a gRPC client streaming request
pub struct GrpcStreamingRequest {
  message_descriptor: MessageDescriptor,
  client_stream: Option<ReceiverStream<Bytes>>,
  tx: Option<Sender<Bytes>>,
}

impl GrpcStreamingRequest {
  /// Create a new GrpcStreamingRequest
  pub fn new(message_descriptor: MessageDescriptor) -> Self {
    let (tx, rx): (Sender<Bytes>, Receiver<Bytes>) = channel(32);
    Self {
      message_descriptor,
      client_stream: Some(ReceiverStream::new(rx)),
      tx: Some(tx),
    }
  }

  /// Restore GrpcStreamingRequest from raw pointer
  pub unsafe fn from_ptr<'a>(
    ptr: crate::grpc_client_streaming_request_handle,
  ) -> Option<&'a mut GrpcStreamingRequest> {
    let req_box = unsafe { &mut *(ptr as *mut Option<GrpcStreamingRequest>) };
    req_box.as_mut()
  }

  /// Get the message descriptor associated with this request
  pub fn get_message_descriptor(&self) -> &MessageDescriptor {
    &self.message_descriptor
  }

  /// Send a message on the streaming request
  pub fn send(&mut self, message: Box<dyn MessageDyn>) -> Result<(), TrySendError<Bytes>> {
    let bytes = message
      .write_to_bytes_dyn()
      .map_err(|_e| {
        // Encode error is mapped to channel Closed; practically shouldn't happen in normal flow
        TrySendError::Closed(Bytes::from_static(b""))
      })
      .unwrap_or_else(|_| Bytes::from_static(b"").to_vec());
    match &self.tx {
      Some(tx) => tx.try_send(bytes.into()),
      None => Err(TrySendError::Closed(Bytes::from_static(b""))),
    }
  }

  /// Mark the streaming request as done (no more messages will be sent)
  pub fn done(&mut self) {
    self.tx = None;
  }

  /// Take the ReceiverStream for consuming pending messages
  pub fn take_stream(&mut self) -> ReceiverStream<Bytes> {
    self.client_stream.take().expect("stream already taken")
  }
}

pub struct GrpcClient {
  /// Type data structures mapped as service/method in gRPC Protobuf
  pub descriptors: HashMap<
    String,
    (
      protobuf::reflect::ServiceDescriptor,
      HashMap<String, Rc<protobuf::reflect::MethodDescriptor>>,
    ),
  >,
  /// Read buffer sent from the I/O side to the Rust side
  pub read_buf: Rc<RefCell<Vec<u8>>>,
  /// Write buffer sent from the Rust side to the I/O side
  pub write_buf: Rc<RefCell<Vec<u8>>>,
  /// Waker triggered when a readable event occurs on the I/O side
  pub waker: Rc<RefCell<Option<Waker>>>,
  /// Task executor
  pub reactor: runtime::Reactor,
  /// Connection for hyper h2
  pub h2_sender: Rc<RefCell<Option<h2::client::SendRequest<Bytes>>>>,
  /// Authority for HTTP/2 requests (e.g., "grpcb.in:9000")
  pub authority: String,
  /// Scheme for HTTP/2 requests (e.g., "http" or "https")
  pub scheme: String,
}

impl GrpcClient {
  /// Restore GrpcClient from raw pointer
  pub unsafe fn from_ptr<'a>(ptr: crate::grpc_client) -> Option<&'a mut GrpcClient> {
    let client_box = unsafe { &mut *(ptr as *mut Option<GrpcClient>) };
    client_box.as_mut()
  }

  /// Add service and method descriptors from a protobuf FileDescriptor
  /// This builds the internal mapping of service names to their methods
  pub fn add_service_and_method(&mut self, fd: FileDescriptor) {
    for service in fd.services() {
      let mut mdm = HashMap::<String, Rc<protobuf::reflect::MethodDescriptor>>::new();
      for method in service.methods() {
        mdm.insert(method.proto().name().to_string(), Rc::new(method));
      }
      self.descriptors.insert(
        format!("{}.{}", fd.package(), service.proto().name()),
        (service, mdm),
      );
    }
  }

  /// Get method descriptor by service and method name
  pub fn get_method_descriptor(
    &self,
    service: &str,
    method: &str,
  ) -> Option<Rc<protobuf::reflect::MethodDescriptor>> {
    self
      .descriptors
      .get(service)
      .and_then(|sd| sd.1.get(method).cloned())
  }

  /// Create tonic::client::Grpc with custom Channel
  pub fn create_tonic_client(&self) -> tonic::client::Grpc<Channel> {
    tonic::client::Grpc::new(Channel::new(
      self.h2_sender.clone(),
      self.authority.clone(),
      self.scheme.clone(),
      self.reactor.clone(),
    ))
  }

  /// Build request path from service and method name
  pub fn build_request_path(
    &self,
    service: &str,
    method: &str,
  ) -> Result<PathAndQuery, InvalidUri> {
    PathAndQuery::from_str(format!("/{}/{}", service, method).as_str())
  }
}
