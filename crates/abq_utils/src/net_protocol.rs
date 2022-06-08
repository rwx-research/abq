//! Simple read/write protocol for abq-related messages across networks.
//! The first 4 bytes of any message is the size of the message (in big-endian order).
//! The rest of the message are the contents, which are serde-serialized json.

use std::io::{Read, Write};

/// Reads a message from a stream communicating with abq.
///
/// Note that [Read::read_exact] is used, and so the stream cannot be non-blocking.
pub fn read<T: serde::de::DeserializeOwned>(reader: &mut impl Read) -> Result<T, std::io::Error> {
    let mut msg_size_buf = [0; 4];
    reader.read_exact(&mut msg_size_buf)?;
    let msg_size = u32::from_be_bytes(msg_size_buf);

    let mut msg_buf = vec![0; msg_size as usize];
    reader.read_exact(&mut msg_buf)?;

    let msg = serde_json::from_slice(&msg_buf)?;
    Ok(msg)
}

/// Writes a message to a stream communicating with abq.
pub fn write<T: serde::Serialize>(writer: &mut impl Write, msg: T) -> Result<(), std::io::Error> {
    let msg_json = serde_json::to_vec(&msg)?;

    let msg_size = msg_json.len();
    let msg_size_buf = u32::to_be_bytes(msg_size as u32);

    let mut msg_buf = Vec::new();
    msg_buf.extend_from_slice(&msg_size_buf);
    msg_buf.extend_from_slice(&msg_json);
    writer.write_all(&msg_buf)?;
    Ok(())
}
