use std::ffi::{CStr, c_char};

pub unsafe fn parse_service_and_method<'a>(
  service: *const c_char,
  method: *const c_char,
) -> (&'a str, &'a str) {
  let service = unsafe { CStr::from_ptr(service).to_str().unwrap() };
  let method = unsafe { CStr::from_ptr(method).to_str().unwrap() };
  (service, method)
}
