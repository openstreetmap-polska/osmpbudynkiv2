/// Binary encoding/decoding for KV store keys and values.
///
/// **Keys** (used as RocksDB lookup keys) are encoded as big-endian i64. RocksDB sorts keys
/// lexicographically, and big-endian byte order makes that sort match numeric order, which
/// enables efficient range scans.
///
/// **Values** (opaque blobs retrieved by exact key lookup, never sorted) use little-endian for
/// all multi-byte integers and floats. Endianness does not affect correctness here; little-endian
/// is native on x86 and avoids unnecessary byte-swapping.

/// Byte length of an encoded node value (lon: 8 bytes LE f64 + lat: 8 bytes LE f64).
pub const NODE_BYTE_LEN: usize = 16;

/// Encode an i64 as 8-byte big-endian (used for all keys).
pub fn encode_key(id: i64) -> [u8; 8] {
    id.to_be_bytes()
}

/// Decode a big-endian key back to i64.
pub fn decode_key(bytes: &[u8]) -> i64 {
    i64::from_be_bytes(bytes.try_into().expect("key must be 8 bytes"))
}

/// Encode node coordinates: (lon, lat) as 16 bytes (two f64).
pub fn encode_node(lon: f64, lat: f64) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&lon.to_le_bytes());
    buf[8..].copy_from_slice(&lat.to_le_bytes());
    buf
}

/// Decode node coordinates from 16 bytes.
pub fn decode_node(bytes: &[u8]) -> (f64, f64) {
    let lon = f64::from_le_bytes(bytes[..8].try_into().unwrap());
    let lat = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
    (lon, lat)
}

/// Encode a list of i64 values (used for way node_ids, reverse indexes).
/// Format: 4-byte length prefix (little-endian u32) + N * 8-byte i64 values.
pub fn encode_id_list(ids: &[i64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + ids.len() * 8);
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for &id in ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    buf
}

/// Decode a list of i64 values.
pub fn decode_id_list(bytes: &[u8]) -> Vec<i64> {
    let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let mut ids = Vec::with_capacity(len);
    for i in 0..len {
        let start = 4 + i * 8;
        ids.push(i64::from_le_bytes(
            bytes[start..start + 8].try_into().unwrap(),
        ));
    }
    ids
}

/// Encode relation members: Vec<(ref_id, member_type, role)>.
/// member_type is encoded as u8: 0=node, 1=way, 2=relation.
/// role is encoded as u8: 0=outer, 1=inner, 2=other (empty string = outer).
/// Format: 4-byte length prefix + N * 10-byte entries (8 byte ref + 1 byte type + 1 byte role).
pub fn encode_member_type(t: &str) -> u8 {
    match t {
        "node" => 0,
        "way" => 1,
        "relation" => 2,
        _ => 3,
    }
}

pub fn decode_member_type(b: u8) -> &'static str {
    match b {
        0 => "node",
        1 => "way",
        2 => "relation",
        _ => "unknown",
    }
}

pub fn encode_member_role(r: &str) -> u8 {
    match r {
        "outer" | "" => 0,
        "inner" => 1,
        _ => 2,
    }
}

pub fn decode_member_role(b: u8) -> &'static str {
    match b {
        0 => "outer",
        1 => "inner",
        _ => "",
    }
}

pub fn encode_relation_members(members: &[(i64, u8, u8)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + members.len() * 10);
    buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
    for &(ref_id, member_type, role) in members {
        buf.extend_from_slice(&ref_id.to_le_bytes());
        buf.push(member_type);
        buf.push(role);
    }
    buf
}

pub fn decode_relation_members(bytes: &[u8]) -> Vec<(i64, u8, u8)> {
    let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let mut members = Vec::with_capacity(len);
    for i in 0..len {
        let start = 4 + i * 10;
        let ref_id = i64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
        let member_type = bytes[start + 8];
        let role = bytes[start + 9];
        members.push((ref_id, member_type, role));
    }
    members
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_roundtrip() {
        for id in [0i64, 1, -1, i64::MAX, i64::MIN, 238_302_933] {
            let encoded = encode_key(id);
            let decoded = decode_key(&encoded);
            assert_eq!(decoded, id);
        }
    }

    #[test]
    fn test_node_roundtrip() {
        let (lon, lat) = (21.014861, 52.206263);
        let encoded = encode_node(lon, lat);
        let (dec_lon, dec_lat) = decode_node(&encoded);
        assert!((dec_lon - lon).abs() < 1e-15);
        assert!((dec_lat - lat).abs() < 1e-15);
    }

    #[test]
    fn test_id_list_roundtrip() {
        let ids = vec![1i64, 2, 3, 100, 238_302_933];
        assert_eq!(decode_id_list(&encode_id_list(&ids)), ids);
    }

    #[test]
    fn test_id_list_empty() {
        let ids: Vec<i64> = vec![];
        assert_eq!(decode_id_list(&encode_id_list(&ids)), ids);
    }

    #[test]
    fn test_relation_members_roundtrip() {
        let members = vec![(10i64, 1u8, 0u8), (11, 1, 1)];
        let decoded = decode_relation_members(&encode_relation_members(&members));
        assert_eq!(decoded, members);
    }

    #[test]
    fn test_member_type_encoding() {
        assert_eq!(decode_member_type(encode_member_type("node")), "node");
        assert_eq!(decode_member_type(encode_member_type("way")), "way");
        assert_eq!(
            decode_member_type(encode_member_type("relation")),
            "relation"
        );
    }

    #[test]
    fn test_member_role_encoding() {
        assert_eq!(decode_member_role(encode_member_role("outer")), "outer");
        assert_eq!(decode_member_role(encode_member_role("inner")), "inner");
    }
}
