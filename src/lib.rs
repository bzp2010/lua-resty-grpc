use bytes::Bytes;
use std::{
  cell::RefCell,
  collections::HashMap,
  ffi::{CString, c_char},
  path::Path,
  rc::Rc,
};
use tokio_stream::StreamExt;

mod client;
mod codec;
mod runtime;
mod transport;

/// Handle to a gRPC client instance
#[allow(non_camel_case_types)]
pub type grpc_client_handle = u64;

/// Handle to a streaming request instance
/// Used for client-side and bidirectional streaming operations
#[allow(non_camel_case_types)]
pub type grpc_client_streaming_request_handle = u64;

/// Callback function type for handling gRPC streaming response done
#[allow(non_camel_case_types)]
pub type grpc_client_on_done = unsafe extern "C" fn();

/// Callback function type for handling gRPC response messages
#[allow(non_camel_case_types)]
pub type grpc_client_on_reply = unsafe extern "C" fn(*const c_char, usize);

#[allow(non_camel_case_types)]
pub type grpc_client_error = ffi_support::ExternError;

thread_local! {
  static GRPC_CLIENT_HANDLES: RefCell<ffi_support::HandleMap<client::GrpcClient>> = RefCell::new(ffi_support::HandleMap::new());
  static GRPC_STREAMING_REQUEST_HANDLES: RefCell<ffi_support::HandleMap<client::GrpcStreamingRequest>> = RefCell::new(ffi_support::HandleMap::new());
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_streaming_request_new(
  client_handle: grpc_client_handle,
  service: ffi_support::FfiStr,
  method: ffi_support::FfiStr,
  err: &mut grpc_client_error,
) -> grpc_client_streaming_request_handle {
  ffi_support::call_with_result(
    err,
    || -> Result<grpc_client_streaming_request_handle, grpc_client_error> {
      let client_handle = ffi_support::Handle::from_u64(client_handle)?;

      let service = service.into_opt_string().ok_or_else(|| {
        grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid service")
      })?;

      let method = method.into_opt_string().ok_or_else(|| {
        grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid method")
      })?;

      GRPC_CLIENT_HANDLES.with(
        move |m| -> Result<grpc_client_streaming_request_handle, grpc_client_error> {
          let client_ref = m.borrow();
          let client = client_ref.get(client_handle)?;

          Ok(GRPC_STREAMING_REQUEST_HANDLES.with(|m| {
            m.borrow_mut()
              .insert(client::GrpcStreamingRequest::new(
                client
                  .get_method_descriptor(&service, &method)
                  .unwrap()
                  .input_type(),
              ))
              .into_u64()
          }))
        },
      )
    },
  )
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_streaming_request_free(
  handle: grpc_client_streaming_request_handle,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || -> Result<(), ffi_support::HandleError> {
    let handle = ffi_support::Handle::from_u64(handle)?;
    GRPC_STREAMING_REQUEST_HANDLES.with(|m| m.borrow_mut().delete(handle))
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_streaming_request_push(
  request_handle: grpc_client_streaming_request_handle,
  data: ffi_support::FfiStr,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || -> Result<(), grpc_client_error> {
    let request_handle = ffi_support::Handle::from_u64(request_handle)?;

    let data = data.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid data")
    })?;

    GRPC_STREAMING_REQUEST_HANDLES.with(move |m| -> Result<(), grpc_client_error> {
      let mut request_ref = m.borrow_mut();
      let request = request_ref.get_mut(request_handle)?;

      let message =
        protobuf_json_mapping::parse_dyn_from_str(request.get_message_descriptor(), &data).unwrap();
      let _ = request.send(message);
      Ok(())
    })
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_streaming_request_done(
  request_handle: grpc_client_streaming_request_handle,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || -> Result<(), grpc_client_error> {
    let request_handle = ffi_support::Handle::from_u64(request_handle)?;

    GRPC_STREAMING_REQUEST_HANDLES.with(move |m| -> Result<(), grpc_client_error> {
      m.borrow_mut().get_mut(request_handle)?.done();
      Ok(())
    })
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_new(
  authority: ffi_support::FfiStr,
  scheme: ffi_support::FfiStr,
  err: &mut grpc_client_error,
) -> grpc_client_handle {
  ffi_support::call_with_result(err, || -> Result<u64, grpc_client_error> {
    let authority = authority.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid authority")
    })?;

    let scheme = scheme.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid scheme")
    })?;

    Ok(GRPC_CLIENT_HANDLES.with(|m| {
      m.borrow_mut()
        .insert(client::GrpcClient {
          descriptors: HashMap::new(),
          read_buf: Rc::new(RefCell::new(Vec::new())),
          write_buf: Rc::new(RefCell::new(Vec::new())),
          waker: Rc::new(RefCell::new(None)),
          reactor: runtime::Reactor::new(),
          h2_sender: Rc::new(RefCell::new(None)),
          authority,
          scheme,
        })
        .into_u64()
    }))
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_free(handle: grpc_client_handle, err: &mut grpc_client_error) {
  ffi_support::call_with_result(err, || -> Result<(), ffi_support::HandleError> {
    let handle = ffi_support::Handle::from_u64(handle)?;
    GRPC_CLIENT_HANDLES.with(|m| m.borrow_mut().delete(handle))
  });
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_load_proto_file(
  handle: grpc_client_handle,
  proto_path: ffi_support::FfiStr,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || -> Result<(), grpc_client_error> {
    let handle = ffi_support::Handle::from_u64(handle)?;

    let path = proto_path.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid proto path")
    })?;

    let path_parent = Path::new(&path).parent().ok_or_else(|| {
      grpc_client_error::new_error(
        ffi_support::ErrorCode::new(1),
        "Failed to get parent directory of proto path",
      )
    })?;

    let file_descriptor_protos = protobuf_parse::Parser::new()
      .pure()
      .include(path_parent)
      .input(path)
      .parse_and_typecheck()
      .map_err(|e| {
        grpc_client_error::new_error(
          ffi_support::ErrorCode::new(1),
          format!("failed to parse proto file: {}", e),
        )
      })?
      .file_descriptors;

    let fd = protobuf::reflect::FileDescriptor::new_dynamic(
      file_descriptor_protos.into_iter().next().unwrap(),
      &[],
    )
    .map_err(|e| {
      grpc_client_error::new_error(
        ffi_support::ErrorCode::new(1),
        format!("failed to create file descriptor: {}", e),
      )
    })?;

    GRPC_CLIENT_HANDLES.with(move |m| -> Result<(), grpc_client_error> {
      m.borrow_mut().get_mut(handle)?.add_service_and_method(fd);
      Ok(())
    })
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_loaded_protos(
  handle: grpc_client_handle,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || {
    let handle = ffi_support::Handle::from_u64(handle)?;

    GRPC_CLIENT_HANDLES.with(|m| -> Result<(), grpc_client_error> {
      let client_ref = m.borrow();
      let client = client_ref.get(handle)?;
      log::debug!(
        "grpc_client_loaded_protos loaded services: {:?}",
        client.descriptors.keys()
      );
      Ok(())
    })
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_connect(handle: grpc_client_handle, err: &mut grpc_client_error) {
  ffi_support::call_with_result(err, || {
    let handle = ffi_support::Handle::from_u64(handle)?;

    GRPC_CLIENT_HANDLES.with(|m| -> Result<(), grpc_client_error> {
      let mut client_ref = m.borrow_mut();
      let client = client_ref.get_mut(handle)?;
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
      Ok(())
    })
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_on_receive(
  handle: grpc_client_handle,
  data: *const c_char,
  n: usize,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || {
    let handle = ffi_support::Handle::from_u64(handle)?;

    GRPC_CLIENT_HANDLES.with(|m| -> Result<(), grpc_client_error> {
      let mut client_ref = m.borrow_mut();
      let client = client_ref.get_mut(handle)?;
      let read_buf = client.read_buf.clone();
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
      Ok(())
    })
  })
}

#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_drive_once(
  handle: grpc_client_handle,
  err: &mut grpc_client_error,
) -> u8 {
  ffi_support::call_with_result(err, || {
    let handle = ffi_support::Handle::from_u64(handle)?;

    GRPC_CLIENT_HANDLES.with(|m| -> Result<bool, ffi_support::HandleError> {
      let mut client_ref = m.borrow_mut();
      let client = client_ref.get_mut(handle)?;
      client.reactor.poke_all();
      Ok(client.reactor.drive_once(64))
    })
  })
}

/// Read pending bytes from Rust buffer and consume them
#[unsafe(no_mangle)]
pub extern "C" fn grpc_client_peek_write(
  handle: grpc_client_handle,
  out: *mut c_char,
  max_len: u32,
  err: &mut grpc_client_error,
) -> u32 {
  ffi_support::call_with_result(err, || {
    let handle = ffi_support::Handle::from_u64(handle)?;

    GRPC_CLIENT_HANDLES.with(|m| -> Result<u32, grpc_client_error> {
      let client_ref = m.borrow();
      let client = client_ref.get(handle)?;
      let mut wb = client.write_buf.borrow_mut();
      let n = std::cmp::min(max_len as usize, wb.len());
      if n == 0 {
        return Ok(0);
      }
      unsafe {
        std::ptr::copy_nonoverlapping(wb.as_ptr(), out as *mut u8, n);
      }

      // Consume the bytes directly
      wb.drain(0..n);
      Ok(n as u32)
    })
  })
}

/// Send a unary request
/// # Safety
/// The callback is passed using the unsafe function. Ensure that you pass in a valid function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_unary(
  handle: grpc_client_handle,
  service: ffi_support::FfiStr,
  method: ffi_support::FfiStr,
  data: ffi_support::FfiStr,
  on_reply_cb: grpc_client_on_reply,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || {
    let handle = ffi_support::Handle::from_u64(handle)?;

    let service = service.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid service")
    })?;

    let method = method.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid method")
    })?;

    let data = data.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid data")
    })?;

    GRPC_CLIENT_HANDLES.with(move |m| -> Result<(), grpc_client_error> {
      let client_ref = m.borrow();
      let client = client_ref.get(handle)?;

      let md = client
        .get_method_descriptor(&service, &method)
        .ok_or_else(|| {
          grpc_client_error::new_error(
            ffi_support::ErrorCode::new(1),
            "failed to get method descriptor",
          )
        })?;

      let mut grpc = client.create_tonic_client();
      let path = client.build_request_path(&service, &method).unwrap();
      let req_bytes = {
        let msg = protobuf_json_mapping::parse_dyn_from_str(&md.input_type(), &data)
          .unwrap()
          .write_to_bytes_dyn()
          .unwrap();
        Bytes::from(msg)
      };

      let reactor = client.reactor.clone();
      reactor.spawn(async move {
        match grpc
          .unary(
            tonic::Request::new(req_bytes),
            path,
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
      Ok(())
    })
  })
}

/// Send a server-side streaming request
/// # Safety
/// The callback is passed using the unsafe function. Ensure that you pass in a valid function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_server_streaming(
  handle: grpc_client_handle,
  service: ffi_support::FfiStr,
  method: ffi_support::FfiStr,
  data: ffi_support::FfiStr,
  on_reply_cb: grpc_client_on_reply,
  on_done_cb: grpc_client_on_done,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || {
    let handle = ffi_support::Handle::from_u64(handle)?;

    let service = service.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid service")
    })?;

    let method = method.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid method")
    })?;

    let data = data.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid data")
    })?;

    GRPC_CLIENT_HANDLES.with(move |m| -> Result<(), grpc_client_error> {
      let client_ref = m.borrow();
      let client = client_ref.get(handle)?;

      let md = client
        .get_method_descriptor(&service, &method)
        .ok_or_else(|| {
          grpc_client_error::new_error(
            ffi_support::ErrorCode::new(1),
            "failed to get method descriptor",
          )
        })?;

      let mut grpc = client.create_tonic_client();
      let path = client.build_request_path(&service, &method).unwrap();
      let req_bytes = {
        let msg = protobuf_json_mapping::parse_dyn_from_str(&md.input_type(), &data)
          .unwrap()
          .write_to_bytes_dyn()
          .unwrap();
        Bytes::from(msg)
      };

      let reactor = client.reactor.clone();
      reactor.spawn(async move {
        match grpc
          .server_streaming(
            tonic::Request::new(req_bytes),
            path,
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
      Ok(())
    })
  })
}

/// Send a client-side streaming request
/// # Safety
/// The callback is passed using the unsafe function. Ensure that you pass in a valid function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_client_streaming(
  client_handle: grpc_client_handle,
  service: ffi_support::FfiStr,
  method: ffi_support::FfiStr,
  request_handle: grpc_client_streaming_request_handle,
  on_reply_cb: grpc_client_on_reply,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || {
    let client_handle = ffi_support::Handle::from_u64(client_handle)?;

    let service = service.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid service")
    })?;

    let method = method.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid method")
    })?;

    GRPC_CLIENT_HANDLES.with(move |m| -> Result<(), grpc_client_error> {
      let client_ref = m.borrow();
      let client = client_ref.get(client_handle)?;

      let md = client
        .get_method_descriptor(&service, &method)
        .ok_or_else(|| {
          grpc_client_error::new_error(
            ffi_support::ErrorCode::new(1),
            "failed to get method descriptor",
          )
        })?;

      let mut grpc = client.create_tonic_client();
      let path = client.build_request_path(&service, &method).unwrap();

      let request = GRPC_STREAMING_REQUEST_HANDLES.with(|m| {
        let handle = ffi_support::Handle::from_u64(request_handle).unwrap();
        let mut map = m.borrow_mut();
        let req = map.get_mut(handle).unwrap();
        req.take_stream()
      });

      let reactor = client.reactor.clone();
      reactor.spawn(async move {
        match grpc
          .client_streaming(tonic::Request::new(request), path, codec::MyCodec::new(md))
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
      Ok(())
    })
  })
}

/// Send a bidirectional streaming request
/// # Safety
/// The callback is passed using the unsafe function. Ensure that you pass in a valid function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grpc_client_streaming(
  client_handle: grpc_client_handle,
  service: ffi_support::FfiStr,
  method: ffi_support::FfiStr,
  request_handle: grpc_client_streaming_request_handle,
  on_reply_cb: grpc_client_on_reply,
  on_done_cb: grpc_client_on_done,
  err: &mut grpc_client_error,
) {
  ffi_support::call_with_result(err, || {
    let client_handle = ffi_support::Handle::from_u64(client_handle)?;

    let service = service.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid service")
    })?;

    let method = method.into_opt_string().ok_or_else(|| {
      grpc_client_error::new_error(ffi_support::ErrorCode::new(1), "invalid method")
    })?;

    GRPC_CLIENT_HANDLES.with(move |m| -> Result<(), grpc_client_error> {
      let client_ref = m.borrow();
      let client = client_ref.get(client_handle)?;

      let md = client
        .get_method_descriptor(&service, &method)
        .ok_or_else(|| {
          grpc_client_error::new_error(
            ffi_support::ErrorCode::new(1),
            "failed to get method descriptor",
          )
        })?;

      let mut grpc = client.create_tonic_client();
      let path = client.build_request_path(&service, &method).unwrap();

      let request = GRPC_STREAMING_REQUEST_HANDLES.with(|m| {
        let handle = ffi_support::Handle::from_u64(request_handle).unwrap();
        let mut map = m.borrow_mut();
        let req = map.get_mut(handle).unwrap();
        tonic::Request::new(req.take_stream())
      });

      let reactor = client.reactor.clone();
      reactor.spawn(async move {
        match grpc.streaming(request, path, codec::MyCodec::new(md)).await {
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
      Ok(())
    })
  })
}
