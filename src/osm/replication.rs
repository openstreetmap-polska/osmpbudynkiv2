use anyhow::{Context, Result, bail};
use quick_xml::events::Event;
use quick_xml::reader::Reader;

/// Represents an action from an OsmChange file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeAction {
    Create,
    Modify,
    Delete,
}

/// A changed node from the OsmChange file.
#[derive(Debug, Clone)]
pub struct NodeChange {
    pub action: ChangeAction,
    pub id: i64,
    /// The element's `version` attribute; 0 when absent. See
    /// [`OsmChange::collapse`].
    pub version: u64,
    pub lon: f64,
    pub lat: f64,
    pub tags: Vec<(String, String)>,
}

/// A changed way from the OsmChange file.
#[derive(Debug, Clone)]
pub struct WayChange {
    pub action: ChangeAction,
    pub id: i64,
    /// The element's `version` attribute; 0 when absent. See
    /// [`OsmChange::collapse`].
    pub version: u64,
    pub node_refs: Vec<i64>,
    pub tags: Vec<(String, String)>,
}

/// A member of a relation.
#[derive(Debug, Clone)]
pub struct RelationMember {
    pub member_type: String,
    pub member_ref: i64,
    pub role: String,
}

/// A changed relation from the OsmChange file.
#[derive(Debug, Clone)]
pub struct RelationChange {
    pub action: ChangeAction,
    pub id: i64,
    /// The element's `version` attribute; 0 when absent. See
    /// [`OsmChange::collapse`].
    pub version: u64,
    pub members: Vec<RelationMember>,
    pub tags: Vec<(String, String)>,
}

/// All changes parsed from an OsmChange file.
#[derive(Debug, Default)]
pub struct OsmChange {
    pub nodes: Vec<NodeChange>,
    pub ways: Vec<WayChange>,
    pub relations: Vec<RelationChange>,
}

impl OsmChange {
    /// Reduce `parts`, a sequence of diffs given oldest first, to **one change
    /// per object**: the one with the highest `version`. Ties, including the
    /// all-zero case of a feed that omits `version`, go to the one that comes
    /// last in `parts` and in document order, which means the sort must be
    /// stable.
    ///
    /// Why dropping the intermediate versions loses nothing: applying a diff
    /// takes each object from its *stored* state to its *final* one, and every
    /// write depends only on those two. Tags and rows come from the final
    /// version. The reverse indexes drop the stored refs and add the final
    /// ones, and an intermediate ref list was never stored. Dirty cells cover
    /// the stored row's cell and the final row's cell, and an intermediate
    /// position was never served. Create followed by delete becomes a delete
    /// of something never stored, which is a no-op. Delete followed by
    /// undelete becomes a modify.
    ///
    /// This makes "one change per id" structural, rather than depending on
    /// which lookup (`find` or `rfind`) the rebuild uses. It also means
    /// versions are ordered by number rather than by file position, which
    /// the feed does not promise. Collapsing a whole batch instead of each
    /// sequence gives the same result with fewer writes, so a node edited in
    /// 20 sequences of a catch-up is written once. The output is sorted by
    /// id, not in document order; objects of one type never depend on each
    /// other's application order, since every geometry rebuild runs after
    /// all of them are applied.
    pub fn collapse<'a>(parts: impl IntoIterator<Item = &'a OsmChange> + Clone) -> OsmChange {
        OsmChange {
            nodes: latest_per_id(parts.clone().into_iter().flat_map(|c| &c.nodes), |n| {
                (n.id, n.version)
            }),
            ways: latest_per_id(parts.clone().into_iter().flat_map(|c| &c.ways), |w| {
                (w.id, w.version)
            }),
            relations: latest_per_id(parts.into_iter().flat_map(|c| &c.relations), |r| {
                (r.id, r.version)
            }),
        }
    }
}

/// Stable-sort by `(id, version)` and keep the last entry of each id.
fn latest_per_id<'a, T: Clone + 'a>(
    items: impl Iterator<Item = &'a T>,
    key: impl Fn(&T) -> (i64, u64),
) -> Vec<T> {
    let mut sorted: Vec<&T> = items.collect();
    sorted.sort_by_key(|t| key(t));
    sorted
        .iter()
        .enumerate()
        .filter(|(i, t)| sorted.get(i + 1).is_none_or(|next| key(next).0 != key(t).0))
        .map(|(_, t)| (*t).clone())
        .collect()
}

/// Parse an OsmChange XML string into structured changes.
pub fn parse_osc(xml: &str) -> Result<OsmChange> {
    let mut reader = Reader::from_str(xml);
    let mut change = OsmChange::default();
    let mut current_action: Option<ChangeAction> = None;

    // State for building current element
    let mut current_node: Option<NodeChange> = None;
    let mut current_way: Option<WayChange> = None;
    let mut current_relation: Option<RelationChange> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let qname = e.name();
                let name =
                    std::str::from_utf8(qname.as_ref()).context("Invalid UTF-8 in element name")?;

                match name {
                    "create" => current_action = Some(ChangeAction::Create),
                    "modify" => current_action = Some(ChangeAction::Modify),
                    "delete" => current_action = Some(ChangeAction::Delete),
                    "node" => {
                        let action =
                            current_action.context("Node element outside of action block")?;
                        let mut id = 0i64;
                        let mut version = 0u64;
                        let mut lon = 0.0f64;
                        let mut lat = 0.0f64;
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let val = std::str::from_utf8(&attr.value)?;
                            match key {
                                "id" => id = val.parse()?,
                                "version" => version = val.parse()?,
                                "lon" => lon = val.parse()?,
                                "lat" => lat = val.parse()?,
                                _ => {}
                            }
                        }
                        current_node = Some(NodeChange {
                            action,
                            id,
                            version,
                            lon,
                            lat,
                            tags: Vec::new(),
                        });
                    }
                    "way" => {
                        let action =
                            current_action.context("Way element outside of action block")?;
                        let mut id = 0i64;
                        let mut version = 0u64;
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let val = std::str::from_utf8(&attr.value)?;
                            match key {
                                "id" => id = val.parse()?,
                                "version" => version = val.parse()?,
                                _ => {}
                            }
                        }
                        current_way = Some(WayChange {
                            action,
                            id,
                            version,
                            node_refs: Vec::new(),
                            tags: Vec::new(),
                        });
                    }
                    "relation" => {
                        let action =
                            current_action.context("Relation element outside of action block")?;
                        let mut id = 0i64;
                        let mut version = 0u64;
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let val = std::str::from_utf8(&attr.value)?;
                            match key {
                                "id" => id = val.parse()?,
                                "version" => version = val.parse()?,
                                _ => {}
                            }
                        }
                        current_relation = Some(RelationChange {
                            action,
                            id,
                            version,
                            members: Vec::new(),
                            tags: Vec::new(),
                        });
                    }
                    _ => {}
                }
            }
            Ok(Event::Empty(ref e)) => {
                let qname = e.name();
                let name =
                    std::str::from_utf8(qname.as_ref()).context("Invalid UTF-8 in element name")?;

                match name {
                    "tag" => {
                        let mut k = String::new();
                        let mut v = String::new();
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let val = std::str::from_utf8(&attr.value)?;
                            match key {
                                "k" => k = val.to_string(),
                                "v" => v = val.to_string(),
                                _ => {}
                            }
                        }
                        if let Some(ref mut node) = current_node {
                            node.tags.push((k, v));
                        } else if let Some(ref mut way) = current_way {
                            way.tags.push((k, v));
                        } else if let Some(ref mut rel) = current_relation {
                            rel.tags.push((k, v));
                        }
                    }
                    "nd" => {
                        if let Some(ref mut way) = current_way {
                            for attr in e.attributes().flatten() {
                                let key = std::str::from_utf8(attr.key.as_ref())?;
                                let val = std::str::from_utf8(&attr.value)?;
                                if key == "ref" {
                                    way.node_refs.push(val.parse()?);
                                }
                            }
                        }
                    }
                    "member" => {
                        if let Some(ref mut rel) = current_relation {
                            let mut member_type = String::new();
                            let mut member_ref = 0i64;
                            let mut role = String::new();
                            for attr in e.attributes().flatten() {
                                let key = std::str::from_utf8(attr.key.as_ref())?;
                                let val = std::str::from_utf8(&attr.value)?;
                                match key {
                                    "type" => member_type = val.to_string(),
                                    "ref" => member_ref = val.parse()?,
                                    "role" => role = val.to_string(),
                                    _ => {}
                                }
                            }
                            rel.members.push(RelationMember {
                                member_type,
                                member_ref,
                                role,
                            });
                        }
                    }
                    "node" => {
                        // Self-closing node (e.g., in delete blocks)
                        let action =
                            current_action.context("Node element outside of action block")?;
                        let mut id = 0i64;
                        let mut version = 0u64;
                        let mut lon = 0.0f64;
                        let mut lat = 0.0f64;
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let val = std::str::from_utf8(&attr.value)?;
                            match key {
                                "id" => id = val.parse()?,
                                "version" => version = val.parse()?,
                                "lon" => lon = val.parse()?,
                                "lat" => lat = val.parse()?,
                                _ => {}
                            }
                        }
                        change.nodes.push(NodeChange {
                            action,
                            id,
                            version,
                            lon,
                            lat,
                            tags: Vec::new(),
                        });
                    }
                    "way" => {
                        // Self-closing way (e.g., in delete blocks)
                        let action =
                            current_action.context("Way element outside of action block")?;
                        let mut id = 0i64;
                        let mut version = 0u64;
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let val = std::str::from_utf8(&attr.value)?;
                            match key {
                                "id" => id = val.parse()?,
                                "version" => version = val.parse()?,
                                _ => {}
                            }
                        }
                        change.ways.push(WayChange {
                            action,
                            id,
                            version,
                            node_refs: Vec::new(),
                            tags: Vec::new(),
                        });
                    }
                    "relation" => {
                        // Self-closing relation (e.g., in delete blocks)
                        let action =
                            current_action.context("Relation element outside of action block")?;
                        let mut id = 0i64;
                        let mut version = 0u64;
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.as_ref())?;
                            let val = std::str::from_utf8(&attr.value)?;
                            match key {
                                "id" => id = val.parse()?,
                                "version" => version = val.parse()?,
                                _ => {}
                            }
                        }
                        change.relations.push(RelationChange {
                            action,
                            id,
                            version,
                            members: Vec::new(),
                            tags: Vec::new(),
                        });
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let qname = e.name();
                let name =
                    std::str::from_utf8(qname.as_ref()).context("Invalid UTF-8 in element name")?;

                match name {
                    "create" | "modify" | "delete" => current_action = None,
                    "node" => {
                        if let Some(node) = current_node.take() {
                            change.nodes.push(node);
                        }
                    }
                    "way" => {
                        if let Some(way) = current_way.take() {
                            change.ways.push(way);
                        }
                    }
                    "relation" => {
                        if let Some(rel) = current_relation.take() {
                            change.relations.push(rel);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => bail!("Error parsing OsmChange XML: {e}"),
            _ => {}
        }
    }

    Ok(change)
}

/// Parse replication state.txt, returning (sequence_number, timestamp).
/// The timestamp colons are escaped in state files (`\:`) and are unescaped here.
pub fn parse_state_txt(text: &str) -> Result<(u64, String)> {
    let mut seq = None;
    let mut timestamp = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("sequenceNumber=") {
            seq = Some(
                value
                    .parse::<u64>()
                    .context("Failed to parse sequence number")?,
            );
        } else if let Some(value) = line.strip_prefix("timestamp=") {
            timestamp = Some(value.replace("\\:", ":"));
        }
    }
    match (seq, timestamp) {
        (Some(s), Some(t)) => Ok((s, t)),
        (None, _) => bail!("No sequenceNumber found in state.txt"),
        (_, None) => bail!("No timestamp found in state.txt"),
    }
}

/// Construct the URL for an OsmChange file given a base URL and sequence number.
pub fn sequence_to_path(seq: u64) -> String {
    let s = format!("{seq:09}");
    format!("{}/{}/{}.osc.gz", &s[0..3], &s[3..6], &s[6..9])
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_OSC: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6">
  <create>
    <node id="100" lon="20.0" lat="50.0" version="1" changeset="1">
      <tag k="name" v="Test Node"/>
      <tag k="addr:housenumber" v="42"/>
    </node>
    <way id="200" version="1" changeset="1">
      <nd ref="1"/>
      <nd ref="2"/>
      <nd ref="3"/>
      <nd ref="1"/>
      <tag k="building" v="yes"/>
    </way>
  </create>
  <modify>
    <node id="101" lon="21.0" lat="51.0" version="2" changeset="2">
      <tag k="name" v="Modified Node"/>
    </node>
  </modify>
  <delete>
    <node id="102" lon="0" lat="0" version="3" changeset="3"/>
    <way id="201" version="2" changeset="3"/>
  </delete>
</osmChange>"#;

    #[test]
    fn test_parse_osc_create_nodes() -> Result<()> {
        let change = parse_osc(SAMPLE_OSC)?;

        let created_nodes: Vec<_> = change
            .nodes
            .iter()
            .filter(|n| n.action == ChangeAction::Create)
            .collect();
        assert_eq!(created_nodes.len(), 1);
        assert_eq!(created_nodes[0].id, 100);
        assert!((created_nodes[0].lon - 20.0).abs() < 1e-9);
        assert!((created_nodes[0].lat - 50.0).abs() < 1e-9);
        assert_eq!(created_nodes[0].tags.len(), 2);
        assert_eq!(
            created_nodes[0].tags[0],
            ("name".into(), "Test Node".into())
        );

        Ok(())
    }

    #[test]
    fn test_parse_osc_create_ways() -> Result<()> {
        let change = parse_osc(SAMPLE_OSC)?;

        let created_ways: Vec<_> = change
            .ways
            .iter()
            .filter(|w| w.action == ChangeAction::Create)
            .collect();
        assert_eq!(created_ways.len(), 1);
        assert_eq!(created_ways[0].id, 200);
        assert_eq!(created_ways[0].node_refs, vec![1, 2, 3, 1]);
        assert_eq!(created_ways[0].tags.len(), 1);
        assert_eq!(created_ways[0].tags[0], ("building".into(), "yes".into()));

        Ok(())
    }

    #[test]
    fn test_parse_osc_modify() -> Result<()> {
        let change = parse_osc(SAMPLE_OSC)?;

        let modified_nodes: Vec<_> = change
            .nodes
            .iter()
            .filter(|n| n.action == ChangeAction::Modify)
            .collect();
        assert_eq!(modified_nodes.len(), 1);
        assert_eq!(modified_nodes[0].id, 101);

        Ok(())
    }

    #[test]
    fn test_parse_osc_delete() -> Result<()> {
        let change = parse_osc(SAMPLE_OSC)?;

        let deleted_nodes: Vec<_> = change
            .nodes
            .iter()
            .filter(|n| n.action == ChangeAction::Delete)
            .collect();
        assert_eq!(deleted_nodes.len(), 1);
        assert_eq!(deleted_nodes[0].id, 102);

        let deleted_ways: Vec<_> = change
            .ways
            .iter()
            .filter(|w| w.action == ChangeAction::Delete)
            .collect();
        assert_eq!(deleted_ways.len(), 1);
        assert_eq!(deleted_ways[0].id, 201);

        Ok(())
    }

    /// One `.osc` can carry an object's whole lifecycle. `OsmChange::collapse`
    /// relies on every version surviving parsing, in document order and with
    /// its own action, coordinates and tags -- including a self-closing
    /// untagged node inside `<modify>`, the commonest shape in real diffs.
    #[test]
    fn test_parse_osc_keeps_every_version_in_document_order() -> Result<()> {
        let change = parse_osc(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6">
  <create>
    <node id="2" version="1" lon="7.0" lat="7.0"><tag k="amenity" v="bench"/></node>
  </create>
  <modify>
    <node id="2" version="2" lon="3.0" lat="3.0"/>
  </modify>
  <delete>
    <node id="2" version="3"/>
  </delete>
  <modify>
    <node id="2" version="4" lon="8.0" lat="8.0"><tag k="natural" v="tree"/></node>
    <way id="1" version="2"><nd ref="2"/><tag k="building" v="yes"/></way>
    <way id="1" version="3"><nd ref="2"/><nd ref="3"/><tag k="building" v="house"/></way>
  </modify>
</osmChange>"#,
        )?;

        let nodes: Vec<_> = change
            .nodes
            .iter()
            .map(|n| (n.id, n.action, n.lon, n.tags.clone()))
            .collect();
        assert_eq!(
            nodes,
            vec![
                (
                    2,
                    ChangeAction::Create,
                    7.0,
                    vec![("amenity".into(), "bench".into())]
                ),
                (2, ChangeAction::Modify, 3.0, vec![]),
                (2, ChangeAction::Delete, 0.0, vec![]),
                (
                    2,
                    ChangeAction::Modify,
                    8.0,
                    vec![("natural".into(), "tree".into())]
                ),
            ]
        );
        let ways: Vec<_> = change
            .ways
            .iter()
            .map(|w| (w.id, w.node_refs.clone(), w.tags.clone()))
            .collect();
        assert_eq!(
            ways,
            vec![
                (1, vec![2], vec![("building".into(), "yes".into())]),
                (1, vec![2, 3], vec![("building".into(), "house".into())]),
            ]
        );
        Ok(())
    }

    /// `collapse` orders versions by number, not by position in the file, and
    /// keeps exactly one change per object.
    #[test]
    fn collapse_keeps_the_highest_version_of_each_object_regardless_of_file_order() -> Result<()> {
        let change = parse_osc(
            r#"<osmChange version="0.6">
  <modify>
    <way id="1" version="3"><nd ref="9"/><tag k="building" v="house"/></way>
    <way id="1" version="2"><nd ref="8"/><tag k="building" v="yes"/></way>
    <way id="7" version="1"><nd ref="7"/></way>
  </modify>
  <create>
    <node id="2" version="1" lon="1.0" lat="1.0"/>
  </create>
  <delete>
    <node id="2" version="2"/>
  </delete>
</osmChange>"#,
        )?;
        let collapsed = OsmChange::collapse([&change]);

        let ways: Vec<_> = collapsed
            .ways
            .iter()
            .map(|w| (w.id, w.version, w.node_refs.clone()))
            .collect();
        assert_eq!(ways, vec![(1, 3, vec![9]), (7, 1, vec![7])]);
        let nodes: Vec<_> = collapsed.nodes.iter().map(|n| (n.id, n.action)).collect();
        assert_eq!(nodes, vec![(2, ChangeAction::Delete)]);
        Ok(())
    }

    /// Across a batch, a later sequence's version wins. With no `version`
    /// attribute (every version 0), the last one in batch-then-document order
    /// wins, i.e. the ordering the apply loop used before collapsing existed.
    #[test]
    fn collapse_across_parts_prefers_the_later_part_on_a_version_tie() -> Result<()> {
        let first = parse_osc(
            r#"<osmChange version="0.6"><modify>
    <way id="5" version="2"><tag k="building" v="yes"/></way>
    <way id="6"><tag k="building" v="a"/></way>
    <way id="6"><tag k="building" v="b"/></way>
</modify></osmChange>"#,
        )?;
        let second = parse_osc(
            r#"<osmChange version="0.6"><modify>
    <way id="5" version="3"><tag k="building" v="house"/></way>
    <way id="6"><tag k="building" v="c"/></way>
</modify></osmChange>"#,
        )?;
        let collapsed = OsmChange::collapse([&first, &second]);

        let ways: Vec<_> = collapsed
            .ways
            .iter()
            .map(|w| (w.id, w.tags[0].1.as_str()))
            .collect();
        assert_eq!(ways, vec![(5, "house"), (6, "c")]);
        Ok(())
    }

    #[test]
    fn test_parse_state_txt() -> Result<()> {
        let state = "\
#Mon Mar 10 12:00:00 UTC 2025
sequenceNumber=6543210
timestamp=2025-03-10T12\\:00\\:00Z";

        let (seq, timestamp) = parse_state_txt(state)?;
        assert_eq!(seq, 6543210);
        assert_eq!(timestamp, "2025-03-10T12:00:00Z");
        Ok(())
    }

    #[test]
    fn test_parse_state_txt_fixture() -> Result<()> {
        let text = std::fs::read_to_string("fixtures/osm.state.txt")
            .expect("fixtures/osm.state.txt should exist");
        let (seq, timestamp) = parse_state_txt(&text)?;
        assert_eq!(seq, 7028130);
        assert_eq!(timestamp, "2026-03-15T16:32:56Z");
        Ok(())
    }

    #[test]
    fn test_parse_osc_fixture() -> Result<()> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let file =
            std::fs::File::open("fixtures/osm.osc.gz").expect("fixtures/osm.osc.gz should exist");
        let mut decoder = GzDecoder::new(file);
        let mut xml = String::new();
        decoder.read_to_string(&mut xml)?;

        let change = parse_osc(&xml)?;

        assert_eq!(change.nodes.len(), 173);
        assert_eq!(change.ways.len(), 49);
        assert_eq!(change.relations.len(), 3);

        // Verify action counts
        let created = change
            .nodes
            .iter()
            .filter(|n| n.action == ChangeAction::Create)
            .count()
            + change
                .ways
                .iter()
                .filter(|w| w.action == ChangeAction::Create)
                .count()
            + change
                .relations
                .iter()
                .filter(|r| r.action == ChangeAction::Create)
                .count();
        assert!(created > 0, "Should have some creates");

        let deleted = change
            .nodes
            .iter()
            .filter(|n| n.action == ChangeAction::Delete)
            .count()
            + change
                .ways
                .iter()
                .filter(|w| w.action == ChangeAction::Delete)
                .count()
            + change
                .relations
                .iter()
                .filter(|r| r.action == ChangeAction::Delete)
                .count();
        assert!(deleted > 0, "Should have some deletes");

        let modified = change
            .nodes
            .iter()
            .filter(|n| n.action == ChangeAction::Modify)
            .count()
            + change
                .ways
                .iter()
                .filter(|w| w.action == ChangeAction::Modify)
                .count()
            + change
                .relations
                .iter()
                .filter(|r| r.action == ChangeAction::Modify)
                .count();
        assert!(modified > 0, "Should have some modifies");

        assert_eq!(created + deleted + modified, 173 + 49 + 3);

        Ok(())
    }

    #[test]
    fn test_sequence_to_path() {
        assert_eq!(sequence_to_path(6543210), "006/543/210.osc.gz");
        assert_eq!(sequence_to_path(1), "000/000/001.osc.gz");
        assert_eq!(sequence_to_path(123456789), "123/456/789.osc.gz");
    }
}
