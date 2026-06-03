package phases

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// patchPatroniConfig sets scope and postgresql.{data_dir,bin_dir,config_dir} (and
// config_dir when configDir != "") in a Patroni YAML document, preserving
// comments, key order, and all other keys. Missing keys (or a missing
// postgresql mapping) are inserted. configDir == "" leaves config_dir untouched.
func patchPatroniConfig(in []byte, scope, dataDir, binDir, configDir string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(in, &doc); err != nil {
		return nil, fmt.Errorf("parse patroni config: %w", err)
	}
	// A non-empty document has one child: the root mapping node.
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, fmt.Errorf("patroni config is empty")
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("patroni config root is not a mapping")
	}

	setMapValue(root, "scope", scope)

	pg := mapValueNode(root, "postgresql")
	switch {
	case pg == nil:
		pg = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMapEntry(root, "postgresql", pg)
	case pg.Kind != yaml.MappingNode:
		// postgresql exists but is null/scalar (a broken config). Replace it in
		// place with a fresh mapping so the PG17 keys land instead of being
		// silently dropped. pg is the value pointer stored in root.Content, so
		// overwriting its pointee replaces the value node.
		*pg = yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	}
	setMapValue(pg, "data_dir", dataDir)
	setMapValue(pg, "bin_dir", binDir)
	if configDir != "" {
		setMapValue(pg, "config_dir", configDir)
	}

	out, err := yaml.Marshal(&doc)
	if err != nil {
		return nil, fmt.Errorf("re-encode patroni config: %w", err)
	}
	return out, nil
}

// mapValueNode returns the value node for key in a mapping node, or nil. A
// mapping node's Content is [key0, val0, key1, val1, ...].
func mapValueNode(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// setMapValue updates key's scalar value in place when present (preserving any
// comment on the value node), otherwise appends a new scalar entry.
func setMapValue(m *yaml.Node, key, value string) {
	if v := mapValueNode(m, key); v != nil {
		v.Kind = yaml.ScalarNode
		v.Tag = "!!str"
		v.Value = value
		return
	}
	appendMapEntry(m, key, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value})
}

// appendMapEntry adds a key/value pair to the end of a mapping node.
func appendMapEntry(m *yaml.Node, key string, value *yaml.Node) {
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		value)
}
