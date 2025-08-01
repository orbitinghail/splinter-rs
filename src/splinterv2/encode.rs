pub trait Encodable {
    fn encoded_size(&self) -> usize;
}
