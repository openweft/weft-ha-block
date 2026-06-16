package main

import "testing"

func TestParseReplica(t *testing.T) {
	cases := []struct {
		in                 string
		name, addr, export string
		wantErr            bool
	}{
		{"r0=10.0.0.1:10809", "r0", "10.0.0.1:10809", "", false},
		{"r1=10.0.0.2:10809/vol-a", "r1", "10.0.0.2:10809", "vol-a", false},
		{"noeq", "", "", "", true},
		{"=addr", "", "", "", true},
		{"name=", "", "", "", true},
		{"r=/onlyexport", "", "", "", true}, // empty addr
	}
	for _, c := range cases {
		name, addr, export, err := parseReplica(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseReplica(%q): want error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseReplica(%q): %v", c.in, err)
			continue
		}
		if name != c.name || addr != c.addr || export != c.export {
			t.Errorf("parseReplica(%q) = (%q,%q,%q); want (%q,%q,%q)", c.in, name, addr, export, c.name, c.addr, c.export)
		}
	}
}

func TestParseVMMap(t *testing.T) {
	if parseVMMap(nil) != nil {
		t.Error("nil specs should yield a nil func (identity sentinel)")
	}
	f := parseVMMap([]string{"node-a=vm-alpha", "garbage", "=bad"})
	if f("node-a") != "vm-alpha" {
		t.Errorf("mapped node-a = %q; want vm-alpha", f("node-a"))
	}
	if f("node-z") != "node-z" {
		t.Errorf("unmapped node-z should fall back to identity; got %q", f("node-z"))
	}
}

func TestConfigValidate(t *testing.T) {
	good := config{
		nodeName: "n", clusterName: "c", etcd: []string{"e"},
		replicas: []string{"r=a:1"}, weftEndpoint: "w:1",
	}
	if err := good.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := []config{
		{},
		{nodeName: "n"},
		{nodeName: "n", clusterName: "c"},
		{nodeName: "n", clusterName: "c", etcd: []string{"e"}},
		{nodeName: "n", clusterName: "c", etcd: []string{"e"}, replicas: []string{"r=a:1"}}, // no weft
	}
	for i, c := range bad {
		if err := c.validate(); err == nil {
			t.Errorf("bad config %d should fail validation", i)
		}
	}
}

func TestIndexByte(t *testing.T) {
	if indexByte("a=b", '=') != 1 {
		t.Error("indexByte = wrong")
	}
	if indexByte("abc", '=') != -1 {
		t.Error("indexByte missing should be -1")
	}
}

func TestRootCmd(t *testing.T) {
	root := rootCmd()
	if root.Use != "weft-ha-block" {
		t.Errorf("root.Use = %q", root.Use)
	}
	// version + agent subcommands present.
	names := map[string]bool{}
	for _, c := range root.Commands() {
		names[c.Name()] = true
	}
	if !names["agent"] || !names["version"] {
		t.Errorf("missing subcommands: %v", names)
	}
}
