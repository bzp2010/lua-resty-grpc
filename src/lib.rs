use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char};
use std::os::raw::{c_int, c_void};
use std::path::Path;
use std::rc::Rc;
use tokio_stream::StreamExt;

mod client;
mod codec;
mod runtime;
mod transport;
mod utils;

/// Handle to a gRPC client instance, opaque pointer for C FFI
#[allow(non_camel_case_types)]
pub type grpc_client = *mut c_void;

/// Handle to a streaming request instance, opaque pointer for C FFI
/// Used for client-side and bidirectional streaming operations
#[allow(non_camel_case_types)]
pub type grpc_client_streaming_request_handle = *mut c_void;

/// Callback function type for handling gRPC streaming response done
#[allow(non_camel_case_types)]
pub type grpc_client_on_done = unsafe extern "C" fn();

/// Callback function type for handling gRPC response messages
#[allow(non_camel_case_types)]
pub type grpc_client_on_reply = unsafe extern "C" fn(*const c_char, usize);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_new_streaming_request(
  handle: grpc_client,
  service: *const c_char,
  method: *const c_char,
) -> grpc_client_streaming_request_handle {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };

  let service = unsafe { CStr::from_ptr(service).to_str().unwrap() };
  let method: &str = unsafe { CStr::from_ptr(method).to_str().unwrap() };
  Box::into_raw(Box::new(client::GrpcStreamingRequest::new(
    client
      .get_method_descriptor(service, method)
      .unwrap()
      .input_type(),
  ))) as grpc_client_streaming_request_handle
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_streaming_request_push(
  req: grpc_client_streaming_request_handle,
  data: *const c_char,
) {
  let request = unsafe { client::GrpcStreamingRequest::from_ptr(req).unwrap() };
  let data = unsafe { CStr::from_ptr(data).to_str().unwrap() };

  let message =
    protobuf_json_mapping::parse_dyn_from_str(request.get_message_descriptor(), data).unwrap();
  let _ = request.send(message);
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_streaming_request_done(req: grpc_client_streaming_request_handle) {
  unsafe { client::GrpcStreamingRequest::from_ptr(req).unwrap().done() };
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_new(authority: *const c_char, scheme: *const c_char) -> grpc_client {
  let authority = unsafe { CStr::from_ptr(authority).to_str().unwrap() };
  let scheme = unsafe { CStr::from_ptr(scheme).to_str().unwrap() };
  Box::into_raw(Box::new(client::GrpcClient {
    descriptors: HashMap::new(),
    read_buf: Rc::new(RefCell::new(Vec::new())),
    write_buf: Rc::new(RefCell::new(Vec::new())),
    waker: Rc::new(RefCell::new(None)),
    reactor: runtime::Reactor::new(),
    h2_sender: Rc::new(RefCell::new(None)),
    authority: authority.to_string(),
    scheme: scheme.to_string(),
  })) as grpc_client
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_load_proto_file(
  handle: grpc_client,
  proto_path: *const c_char,
) -> c_int {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };

  let path = unsafe { CStr::from_ptr(proto_path).to_str().unwrap() };
  let file_descriptor_protos = protobuf_parse::Parser::new()
    .pure()
    .include(Path::new(path).parent().unwrap())
    .input(path)
    .parse_and_typecheck()
    .unwrap()
    .file_descriptors;

  let fd = protobuf::reflect::FileDescriptor::new_dynamic(
    file_descriptor_protos.into_iter().next().unwrap(),
    &[],
  )
  .unwrap();

  client.add_service_and_method(fd);

  1
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_loaded_protos(handle: grpc_client) {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };
  log::debug!(
    "grpc_client_loaded_protos loaded services: {:?}",
    client.descriptors.keys()
  );
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_connect(handle: grpc_client) {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };
  let read_buf = client.read_buf.clone();
  let write_buf = client.write_buf.clone();
  let waker = client.waker.clone();
  let reactor_for_spawn = client.reactor.clone();
  let reactor_for_drive = reactor_for_spawn.clone();
  let sender_cell = client.h2_sender.clone();
  client.reactor.spawn(async move {
    let io = transport::memory_shim::MemoryShim::new(read_buf, write_buf, waker);
    match h2::client::handshake(io).await {
      Ok((send_request, connection)) => {
        log::debug!("h2 handshake OK; storing sender");
        let drive = async move {
          if let Err(e) = connection.await {
            log::error!("h2 connection error: {:?}", e);
          } else {
            log::debug!("h2 connection closed cleanly");
          }
        };
        *sender_cell.borrow_mut() = Some(send_request);
        reactor_for_drive.spawn(drive);
      }
      Err(e) => {
        log::error!("h2 handshake error: {:?}", e);
      }
    }
  });
  client.reactor.drive_once(64);
  log::trace!("grpc_server_connect called");
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_on_receive(handle: grpc_client, data: *const c_char, n: usize) {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };
  let read_buf: Rc<RefCell<Vec<u8>>> = client.read_buf.clone();
  let waker = client.waker.clone();
  let mut b = read_buf.borrow_mut();
  unsafe {
    let bytes = std::slice::from_raw_parts(data as *const u8, n);
    b.extend_from_slice(bytes);
  }
  if let Some(w) = waker.borrow_mut().take() {
    w.wake();
  }
  client.reactor.poke_all();
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_drive_once(handle: grpc_client) -> c_int {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };
  client.reactor.poke_all();
  let has_more = client.reactor.drive_once(64);
  if has_more { 1 } else { 0 }
}

/// Read pending bytes from Rust buffer and consume them
#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_peek_write(
  handle: grpc_client,
  out: *mut c_char,
  max_len: usize,
) -> usize {
  if out.is_null() || max_len == 0 {
    return 0;
  }
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };
  let mut wb = client.write_buf.borrow_mut();
  let n = std::cmp::min(max_len, wb.len());
  if n == 0 {
    return 0;
  }
  unsafe {
    std::ptr::copy_nonoverlapping(wb.as_ptr(), out as *mut u8, n);
  }
  // Consume the bytes directly
  wb.drain(0..n);
  n
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_unary(
  handle: grpc_client,
  service: *const c_char,
  method: *const c_char,
  data: *const c_char,
  on_reply_cb: grpc_client_on_reply,
) {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };

  let (service, method) = unsafe { utils::parse_service_and_method(service, method) };
  let data = unsafe { CStr::from_ptr(data).to_str().unwrap() };
  let md = client.get_method_descriptor(service, method).unwrap();

  let reactor = client.reactor.clone();
  reactor.spawn(async move {
    match client
      .create_tonic_client()
      .unary(
        tonic::Request::new(Bytes::from(
          protobuf_json_mapping::parse_dyn_from_str(&md.input_type(), data)
            .unwrap()
            .write_to_bytes_dyn()
            .unwrap(),
        )),
        client.build_request_path(service, method).unwrap(),
        codec::MyCodec::new(md),
      )
      .await
    {
      Ok(resp) => {
        let json = protobuf_json_mapping::print_to_string(resp.into_inner().as_ref()).unwrap();
        if let Ok(cstr) = CString::new(json) {
          unsafe { on_reply_cb(cstr.as_ptr(), cstr.count_bytes()) };
        }
      }
      Err(status) => {
        log::error!("tonic status error: {:?}", status);
      }
    }
  });
  reactor.drive_once(64);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_server_streaming(
  handle: grpc_client,
  service: *const c_char,
  method: *const c_char,
  data: *const c_char,
  on_reply_cb: grpc_client_on_reply,
  on_done_cb: grpc_client_on_done,
) {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };

  let (service, method) = unsafe { utils::parse_service_and_method(service, method) };
  let data = unsafe { CStr::from_ptr(data).to_str().unwrap() };
  let md = client.get_method_descriptor(service, method).unwrap();

  let reactor = client.reactor.clone();
  reactor.spawn(async move {
    match client
      .create_tonic_client()
      .server_streaming(
        tonic::Request::new(Bytes::from(
          protobuf_json_mapping::parse_dyn_from_str(&md.input_type(), data)
            .unwrap()
            .write_to_bytes_dyn()
            .unwrap(),
        )),
        client.build_request_path(service, method).unwrap(),
        codec::MyCodec::new(md),
      )
      .await
    {
      Ok(resp) => {
        let mut stream = resp.into_inner();
        while let Some(item) = stream.next().await {
          let json = protobuf_json_mapping::print_to_string(item.unwrap().as_ref()).unwrap();
          if let Ok(cstr) = CString::new(json) {
            unsafe { on_reply_cb(cstr.as_ptr(), cstr.count_bytes()) };
          }
        }
        unsafe { on_done_cb() };
      }
      Err(status) => {
        log::error!("tonic status error: {:?}", status);
      }
    }
  });
  reactor.drive_once(64);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_client_streaming(
  handle: grpc_client,
  service: *const c_char,
  method: *const c_char,
  req_handle: grpc_client_streaming_request_handle,
  on_reply_cb: grpc_client_on_reply,
) {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };
  let request = unsafe { client::GrpcStreamingRequest::from_ptr(req_handle).unwrap() };

  let (service, method) = unsafe { utils::parse_service_and_method(service, method) };
  let reactor = client.reactor.clone();
  reactor.spawn(async move {
    match client
      .create_tonic_client()
      .client_streaming(
        tonic::Request::new(request.take_stream()),
        client.build_request_path(service, method).unwrap(),
        codec::MyCodec::new(client.get_method_descriptor(service, method).unwrap()),
      )
      .await
    {
      Ok(resp) => {
        let json = protobuf_json_mapping::print_to_string(resp.into_inner().as_ref()).unwrap();
        if let Ok(cstr) = CString::new(json) {
          unsafe { on_reply_cb(cstr.as_ptr(), cstr.count_bytes()) };
        }
      }
      Err(status) => {
        log::error!("tonic status error: {:?}", status);
      }
    }
  });
  reactor.drive_once(64);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_streaming(
  handle: grpc_client,
  service: *const c_char,
  method: *const c_char,
  req: grpc_client_streaming_request_handle,
  on_reply_cb: grpc_client_on_reply,
  on_done_cb: grpc_client_on_done,
) {
  let client = unsafe { client::GrpcClient::from_ptr(handle).unwrap() };
  let request = unsafe { client::GrpcStreamingRequest::from_ptr(req).unwrap() };

  let (service, method) = unsafe { utils::parse_service_and_method(service, method) };
  let reactor = client.reactor.clone();
  reactor.spawn(async move {
    match client
      .create_tonic_client()
      .streaming(
        tonic::Request::new(request.take_stream()),
        client.build_request_path(service, method).unwrap(),
        codec::MyCodec::new(client.get_method_descriptor(service, method).unwrap()),
      )
      .await
    {
      Ok(resp) => {
        let mut stream = resp.into_inner();
        while let Some(item) = stream.next().await {
          let json = protobuf_json_mapping::print_to_string(item.unwrap().as_ref()).unwrap();
          if let Ok(cstr) = CString::new(json) {
            unsafe { on_reply_cb(cstr.as_ptr(), cstr.count_bytes()) };
          }
        }
        unsafe { on_done_cb() };
      }
      Err(status) => {
        log::error!("tonic status error: {:?}", status);
      }
    }
  });
  reactor.drive_once(64);
}
