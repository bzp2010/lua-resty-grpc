use std::rc::Rc;

use bytes::{Buf, BufMut, Bytes};
use protobuf::{MessageDyn, reflect::MethodDescriptor};

pub struct MyCodec {
  md: Rc<MethodDescriptor>,
}

impl MyCodec {
  pub fn new(md: Rc<MethodDescriptor>) -> Self {
    Self { md }
  }
}

impl tonic::codec::Codec for MyCodec {
  type Encode = Bytes;
  type Decode = Box<dyn MessageDyn>;

  type Encoder = MyEncoder;
  type Decoder = MyDecoder;

  fn encoder(&mut self) -> Self::Encoder {
    MyEncoder {}
  }

  fn decoder(&mut self) -> Self::Decoder {
    MyDecoder {
      md: self.md.clone(),
    }
  }
}

pub struct MyEncoder {}

impl tonic::codec::Encoder for MyEncoder {
  type Item = Bytes;
  type Error = tonic::Status;

  fn encode(
    &mut self,
    item: Self::Item,
    dst: &mut tonic::codec::EncodeBuf,
  ) -> Result<(), Self::Error> {
    dst.put_slice(&item);
    Ok(())
  }
}

pub struct MyDecoder {
  md: Rc<MethodDescriptor>,
}

impl tonic::codec::Decoder for MyDecoder {
  type Item = Box<dyn MessageDyn>;
  type Error = tonic::Status;

  fn decode(
    &mut self,
    src: &mut tonic::codec::DecodeBuf,
  ) -> Result<Option<Self::Item>, Self::Error> {
    let bytes = src.chunk();

    let instance = self
      .md
      .output_type()
      .parse_from_bytes(bytes)
      .map_err(|e| tonic::Status::internal(format!("Failed to decode message: {}", e)))?;
    src.advance(bytes.len());
    Ok(Some(instance))
  }
}

// This library is always loaded by a single worker, and only one thread within that worker will use it.
// Therefore, we assert that this structure will not be shared between threads.
unsafe impl Send for MyDecoder {}
