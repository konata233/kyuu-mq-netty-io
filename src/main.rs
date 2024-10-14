pub mod mq;
mod test;

use crate::mq::protocol::protobase::Deserialize;
use crate::mq::protocol::protobase::Serialize;
use std::io::{Read, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    test::spmc_test::spmc_test()
}


