/// Binary encoding/decoding for KV store keys and values.
///
/// # Key byte order
///
/// Keys are **big-endian**, deliberately. RocksDB sorts keys lexicographically
/// and delta-encodes them within a block, and a block is the unit of I/O. Under
/// little-endian, sorting is by *least* significant byte first, which scatters
/// numerically-adjacent ids across the whole keyspace. A building's nodes are
/// usually consecutive ids (drawn in one editing session), so big-endian keys
/// put them in the same block: one block read per building instead of one per
/// node, plus far better prefix compression. Nothing in this codebase iterates
/// the store in key order (only point lookups and full-range compaction), so
/// the change is invisible to correctness.
///
/// Negative ids would sort above positives when compared as unsigned bytes.
/// That never happens with real OSM data — negative ids are an editor-local
/// convention for unsaved objects and appear in neither a PBF nor a
/// replication diff — and key order is a performance property here, not a
/// correctness one, so plain big-endian is used rather than a sign-flip.
///
/// # Value encoding
///
/// Coordinates are stored as two `i32` decimicrodegrees rather than two `f64`.
/// OSM coordinates live on an exact 1e-7 degree grid and `180 * 1e7` fits in
/// `i32`, so this is lossless and halves the nodes column family. See
/// [`decimicro_to_f64`] for the one subtlety on the way back out.
///
/// Byte length of an encoded node value (lon: 4 bytes LE i32 + lat: 4 bytes LE i32).
pub const NODE_BYTE_LEN: usize = 8;

/// Byte length of a WKB coordinate pair (two LE f64), the widened form node
/// values are expanded into when building geometry.
pub const WKB_COORD_BYTE_LEN: usize = 16;

/// OSM's coordinate grid: 1e-7 degrees, a.k.a. decimicrodegrees.
const COORD_SCALE: f64 = 1e7;

/// Encode an i64 as 8-byte big-endian (used for all keys). See the module doc
/// for why big-endian rather than little.
pub fn encode_key(id: i64) -> [u8; 8] {
    id.to_be_bytes()
}

/// Decode a big-endian key back to i64.
///
/// Test-only: nothing iterates the store in key order, so production never
/// reads a key back. Kept as [`encode_key`]'s round-trip partner.
#[cfg(test)]
pub fn decode_key(bytes: &[u8]) -> i64 {
    i64::from_be_bytes(bytes.try_into().expect("key must be 8 bytes"))
}

/// Convert a degree coordinate to decimicrodegrees.
///
/// Rounds rather than truncates: the `.osc` replication path parses
/// coordinates as decimal text into `f64`, and the nearest `f64` to a value
/// like `21.0148610` can land a hair below it, which truncation would turn
/// into an off-by-one in the last digit.
pub fn f64_to_decimicro(v: f64) -> i32 {
    (v * COORD_SCALE).round() as i32
}

/// Convert decimicrodegrees back to degrees.
///
/// Divides by `1e7` instead of multiplying by `1e-7` on purpose. `1e7` is
/// exactly representable in `f64` so the division rounds once, correctly;
/// `1e-7` is *not* exactly representable, so multiplying rounds twice and can
/// land one ULP off.
pub fn decimicro_to_f64(v: i32) -> f64 {
    f64::from(v) / COORD_SCALE
}

/// Encode node coordinates: (lon, lat) as 8 bytes (two LE i32 decimicrodegrees).
pub fn encode_node(lon_dm: i32, lat_dm: i32) -> [u8; NODE_BYTE_LEN] {
    let mut buf = [0u8; NODE_BYTE_LEN];
    buf[..4].copy_from_slice(&lon_dm.to_le_bytes());
    buf[4..].copy_from_slice(&lat_dm.to_le_bytes());
    buf
}

/// Decode node coordinates from 8 bytes, as decimicrodegrees.
pub fn decode_node(bytes: &[u8]) -> (i32, i32) {
    let lon = i32::from_le_bytes(bytes[..4].try_into().unwrap());
    let lat = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
    (lon, lat)
}

/// Append a node value's coordinates to `out` as a WKB coordinate pair
/// (two LE f64), widening from the stored decimicrodegree form.
///
/// This is the step that replaced a straight `memcpy`. The old `f64` value
/// layout was already byte-identical to WKB's, so coordinates could be copied
/// into a geometry buffer untouched; the `i32` form halves the bytes read from
/// the block cache at the cost of this widening.
pub fn push_wkb_coords(out: &mut Vec<u8>, value: &[u8]) {
    let (lon_dm, lat_dm) = decode_node(value);
    out.extend_from_slice(&decimicro_to_f64(lon_dm).to_le_bytes());
    out.extend_from_slice(&decimicro_to_f64(lat_dm).to_le_bytes());
}

// --- Varint / zigzag primitives ---

fn put_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn get_uvarint(bytes: &[u8], pos: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let b = bytes[*pos];
        *pos += 1;
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn zigzag_encode(v: i64) -> u64 {
    ((v as u64) << 1) ^ ((v >> 63) as u64)
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

// --- Id lists ---
//
// There are two id-list encodings on purpose, and they are not
// interchangeable.
//
// `encode_delta_id_list` (delta + zigzag varint) is used for way node refs,
// where the win is large: a way's node ids are usually near-consecutive, so
// most deltas fit in one byte instead of eight. It preserves order, which is
// mandatory — a way's ref order *is* its polygon vertex order, so these lists
// must never be sorted.
//
// `encode_fixed_id_list` (4-byte count + 8-byte elements) is used for the
// reverse-index column families, which carry a RocksDB merge operator. That
// operator's partial merge concatenates bare 8-byte operands without decoding
// them (see `kvstore::id_list_partial_merge`), which only works on a
// fixed-width format. Switching those to varint means reworking the merge
// operator, and the payoff is much smaller there (average list length is close
// to 1), so they keep the simple format.

/// Encode an ordered list of ids as delta + zigzag varint. Order-preserving.
pub fn encode_delta_id_list(ids: &[i64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + ids.len() * 2);
    put_uvarint(&mut buf, ids.len() as u64);
    let mut prev = 0i64;
    for &id in ids {
        put_uvarint(&mut buf, zigzag_encode(id.wrapping_sub(prev)));
        prev = id;
    }
    buf
}

/// Decode a delta + zigzag varint id list.
pub fn decode_delta_id_list(bytes: &[u8]) -> Vec<i64> {
    let mut pos = 0usize;
    let len = get_uvarint(bytes, &mut pos) as usize;
    let mut ids = Vec::with_capacity(len);
    let mut prev = 0i64;
    for _ in 0..len {
        let delta = zigzag_decode(get_uvarint(bytes, &mut pos));
        prev = prev.wrapping_add(delta);
        ids.push(prev);
    }
    ids
}

/// Encode a list of i64 values as a 4-byte LE count followed by 8-byte LE
/// elements. Used by the reverse-index column families — see the note above on
/// why these do not use the delta encoding.
pub fn encode_fixed_id_list(ids: &[i64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + ids.len() * 8);
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for &id in ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    buf
}

/// Decode a fixed-width id list.
pub fn decode_fixed_id_list(bytes: &[u8]) -> Vec<i64> {
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
///
/// Left fixed-width: there are ~290k relations nationally, so this table is
/// rounding error against nodes and ways and does not justify a second
/// variable-width format to maintain.
pub fn encode_member_type(t: &str) -> u8 {
    match t {
        "node" => 0,
        "way" => 1,
        "relation" => 2,
        _ => 3,
    }
}

/// Test-only round-trip partner of [`encode_member_type`]: member types are
/// decoded by `decode_relation`, which maps the byte itself.
#[cfg(test)]
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

    /// The point of big-endian keys: numerically adjacent ids must also be
    /// lexicographically adjacent, so RocksDB co-locates them in one block.
    /// Little-endian encoding fails this — it sorts by the low byte first.
    #[test]
    fn keys_sort_in_numeric_order_for_positive_ids() {
        let mut keys: Vec<[u8; 8]> = [5i64, 1, 1_000_000_003, 3, 1_000_000_001, 2]
            .iter()
            .map(|&id| encode_key(id))
            .collect();
        keys.sort();
        let decoded: Vec<i64> = keys.iter().map(|k| decode_key(k)).collect();
        assert_eq!(decoded, vec![1, 2, 3, 5, 1_000_000_001, 1_000_000_003]);
    }

    #[test]
    fn test_node_roundtrip() {
        let (lon, lat) = (21.014861, 52.206263);
        let encoded = encode_node(f64_to_decimicro(lon), f64_to_decimicro(lat));
        let (dec_lon, dec_lat) = decode_node(&encoded);
        assert_eq!(dec_lon, 210_148_610);
        assert_eq!(dec_lat, 522_062_630);
        assert!((decimicro_to_f64(dec_lon) - lon).abs() < 1e-9);
        assert!((decimicro_to_f64(dec_lat) - lat).abs() < 1e-9);
    }

    /// A node value is two `i32`s, not two `f64`s -- 8 bytes, not 16. This is
    /// the largest column family in the store, so the width is load-bearing.
    #[test]
    fn node_value_is_eight_bytes() {
        assert_eq!(NODE_BYTE_LEN, 8);
        assert_eq!(encode_node(0, 0).len(), 8);
    }

    /// Every coordinate on OSM's 1e-7 grid must survive a degrees ->
    /// decimicro -> degrees round trip landing back on the same grid point.
    #[test]
    fn coordinate_roundtrip_is_exact_on_the_osm_grid() {
        for dm in [
            0i32,
            1,
            -1,
            210_148_610,
            522_062_630,
            1_800_000_000,
            -1_800_000_000,
        ] {
            assert_eq!(f64_to_decimicro(decimicro_to_f64(dm)), dm, "dm={dm}");
        }
    }

    /// Poland's extreme longitudes/latitudes must be nowhere near i32's range.
    #[test]
    fn full_coordinate_range_fits_in_i32() {
        assert!(f64_to_decimicro(180.0) < i32::MAX);
        assert!(f64_to_decimicro(-180.0) > i32::MIN);
    }

    #[test]
    fn test_delta_id_list_roundtrip() {
        for ids in [
            vec![],
            vec![1i64],
            vec![1i64, 2, 3, 100, 238_302_933],
            // Descending and mixed deltas: a way can revisit lower ids.
            vec![500i64, 499, 1000, 2, 999_999_999_999],
            // A closed ring repeats its first node last.
            vec![10i64, 11, 12, 13, 10],
        ] {
            assert_eq!(decode_delta_id_list(&encode_delta_id_list(&ids)), ids);
        }
    }

    /// Way refs must come back in the order they went in — that order is the
    /// polygon's vertex order, so any reordering silently corrupts geometry.
    #[test]
    fn delta_id_list_preserves_order() {
        let ring = vec![900i64, 100, 500, 300, 900];
        assert_eq!(decode_delta_id_list(&encode_delta_id_list(&ring)), ring);
    }

    /// The reason lever 3 exists: consecutive ids must cost ~1 byte each
    /// rather than 8.
    #[test]
    fn delta_id_list_is_compact_for_consecutive_ids() {
        let ids: Vec<i64> = (1_000_000_000..1_000_000_064).collect();
        let delta = encode_delta_id_list(&ids);
        let fixed = encode_fixed_id_list(&ids);
        assert!(
            delta.len() * 4 < fixed.len(),
            "delta={} fixed={}",
            delta.len(),
            fixed.len()
        );
    }

    #[test]
    fn test_fixed_id_list_roundtrip() {
        let ids = vec![1i64, 2, 3, 100, 238_302_933];
        assert_eq!(decode_fixed_id_list(&encode_fixed_id_list(&ids)), ids);
    }

    #[test]
    fn test_fixed_id_list_empty() {
        let ids: Vec<i64> = vec![];
        assert_eq!(decode_fixed_id_list(&encode_fixed_id_list(&ids)), ids);
    }

    #[test]
    fn test_zigzag_roundtrip() {
        for v in [0i64, 1, -1, 63, -64, i64::MAX, i64::MIN, 238_302_933] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v, "v={v}");
        }
    }

    #[test]
    fn test_uvarint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, u64::MAX] {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, v);
            let mut pos = 0;
            assert_eq!(get_uvarint(&buf, &mut pos), v, "v={v}");
            assert_eq!(pos, buf.len());
        }
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
