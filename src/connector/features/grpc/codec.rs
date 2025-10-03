use bytes::Bytes;
use bytes::{Buf, BufMut};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

/// Специальные кодеки, которые просто передают сырые байты (Bytes) в/из gRPC, без сериализации protobuf.
/// Простой кодек: сообщение = сырые Bytes.
/// Tonic сам добавит/снимет gRPC-фрейминг и компрессию.
#[derive(Clone, Default)]
pub struct RawCodec;

#[derive(Clone, Default)]
pub struct RawEncoder;

#[derive(Clone, Default)]
pub struct RawDecoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        // EncodeBuf реализует BufMut: просто переливаем байты сообщения.
        dst.put_slice(&item);
        Ok(())
    }
}

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        // DecodeBuf реализует Buf: получаем все оставшиеся байты как Bytes.
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}
