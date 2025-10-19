use std::{
  cell::RefCell,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, Waker},
};

use tokio::io::ReadBuf;

pub struct MemoryShim {
  pub read_buf: Rc<RefCell<Vec<u8>>>,
  pub write_buf: Rc<RefCell<Vec<u8>>>,
  pub waker: Rc<RefCell<Option<Waker>>>,
}

impl MemoryShim {
  pub fn new(
    read_buf: Rc<RefCell<Vec<u8>>>,
    write_buf: Rc<RefCell<Vec<u8>>>,
    waker: Rc<RefCell<Option<Waker>>>,
  ) -> Self {
    Self {
      read_buf,
      write_buf,
      waker,
    }
  }
}

impl tokio::io::AsyncRead for MemoryShim {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<std::io::Result<()>> {
    let mut rb = self.read_buf.borrow_mut();
    if rb.is_empty() {
      *self.waker.borrow_mut() = Some(cx.waker().clone());
      // pending until new bytes arrive from C side
      return Poll::Pending;
    }
    let n = std::cmp::min(buf.remaining(), rb.len());
    buf.put_slice(&rb[..n]);
    rb.drain(..n);
    // read fulfilled
    Poll::Ready(Ok(()))
  }
}

impl tokio::io::AsyncWrite for MemoryShim {
  fn poll_write(
    self: Pin<&mut Self>,
    _cx: &mut Context<'_>,
    data: &[u8],
  ) -> Poll<std::io::Result<usize>> {
    self.write_buf.borrow_mut().extend_from_slice(data);
    Poll::Ready(Ok(data.len()))
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    // In the simplified model, we don't need to actually flush anything
    // Data stays in write_buf until Lua side polls for it
    Poll::Ready(Ok(()))
  }

  fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    log::debug!("AsyncWrite poll_shutdown");

    Poll::Ready(Ok(()))
  }
}
