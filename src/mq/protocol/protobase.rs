pub trait Serialize<const N: usize> {
    fn serialize(&self) -> [u8; N];

    fn serialize_vec(&self) -> Vec<u8>;
}

pub trait Deserialize<const N: usize> {
    type T;

    fn deserialize(bytes: [u8; N]) -> Self::T;
}