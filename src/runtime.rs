use std::{
  cell::RefCell,
  collections::VecDeque,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

struct Task {
  fut: Pin<Box<dyn Future<Output = ()> + 'static>>,
  queued: bool,
}
#[derive(Default)]
struct ReactorInner {
  tasks: Vec<Option<Task>>,
  ready: VecDeque<usize>,
}

/// Lightweight single-step task executor
#[derive(Default, Clone)]
pub struct Reactor {
  inner: Rc<RefCell<ReactorInner>>,
}
impl Reactor {
  pub fn new() -> Self {
    Self {
      inner: Rc::new(RefCell::new(ReactorInner::default())),
    }
  }

  /// Spawn a task
  pub fn spawn<F>(&self, fut: F)
  where
    F: Future<Output = ()> + 'static,
  {
    let mut inner = self.inner.borrow_mut();

    // Always append a new slot to avoid clobbering a task currently being polled (whose slot is temporarily None)
    inner.tasks.push(None);
    let id = inner.tasks.len() - 1;
    inner.tasks[id] = Some(Task {
      fut: Box::pin(fut),
      queued: false,
    });
    inner.ready.push_back(id);
    log::trace!(
      "Reactor.spawn -> task {} queued (ready_len={})",
      id,
      inner.ready.len()
    );
  }

  /// Wake up suspended tasks and add them to the ready queue
  pub fn poke_all(&self) {
    let mut inner = self.inner.borrow_mut();
    let mut to_queue: Vec<usize> = Vec::new();
    for (id, slot) in inner.tasks.iter_mut().enumerate() {
      if let Some(task) = slot.as_mut()
        && !task.queued
      {
        task.queued = true;
        to_queue.push(id);
      }
    }
    for id in &to_queue {
      inner.ready.push_back(*id);
    }
    if !to_queue.is_empty() {
      log::trace!("Reactor.poke_all queued tasks: {:?}", to_queue);
    }
  }

  /// Construct a waker to wake up suspended tasks
  fn make_waker(id: usize, inner: Rc<RefCell<ReactorInner>>) -> Waker {
    #[derive(Clone)]
    struct Wd {
      id: usize,
      inner: Rc<RefCell<ReactorInner>>,
    }
    unsafe fn clone(data: *const ()) -> RawWaker {
      let wd = unsafe { &*(data as *const Wd) };
      let boxed = Box::new(wd.clone());
      RawWaker::new(Box::into_raw(boxed) as *const (), &VTABLE)
    }
    unsafe fn wake(data: *const ()) {
      let wd: Box<Wd> = unsafe { Box::from_raw(data as *mut Wd) };
      let mut inner = wd.inner.borrow_mut();
      if let Some(Some(task)) = inner.tasks.get_mut(wd.id)
        && !task.queued
      {
        task.queued = true;
        inner.ready.push_back(wd.id);
      }
    }
    unsafe fn wake_by_ref(data: *const ()) {
      let wd = unsafe { &*(data as *const Wd) };
      let mut inner = wd.inner.borrow_mut();
      if let Some(Some(task)) = inner.tasks.get_mut(wd.id)
        && !task.queued
      {
        task.queued = true;
        inner.ready.push_back(wd.id);
      }
    }
    unsafe fn drop(data: *const ()) {
      let _ = unsafe { Box::from_raw(data as *mut Wd) };
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    let wd = Box::new(Wd { id, inner });
    let raw = RawWaker::new(Box::into_raw(wd) as *const (), &VTABLE);
    unsafe { Waker::from_raw(raw) }
  }

  /// Execute the specified number of poll operations
  pub fn drive_once(&self, max: usize) -> bool {
    let mut polled = 0usize;
    loop {
      if polled >= max {
        break;
      }
      let (id, mut task) = {
        let mut inner = self.inner.borrow_mut();
        match inner.ready.pop_front() {
          Some(id) => {
            log::trace!("Reactor.drive_once -> poll task {}", id);
            let mut task = inner.tasks[id].take().expect("task should exist");
            task.queued = false;
            (id, task)
          }
          None => break,
        }
      };
      let waker = Self::make_waker(id, self.inner.clone());
      let mut cx = Context::from_waker(&waker);
      match task.fut.as_mut().poll(&mut cx) {
        Poll::Ready(()) => {}
        Poll::Pending => {
          let mut inner = self.inner.borrow_mut();
          inner.tasks[id] = Some(task);
        }
      }
      polled += 1;
    }
    let inner = self.inner.borrow();
    !inner.ready.is_empty()
  }
}
