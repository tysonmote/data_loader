package main

import "testing"

func TestLoadAllSpecs(t *testing.T) {
	specs, err := LoadAllSpecs("./fixtures")
	if err != nil {
		t.Fatal(err)
	}

	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}

	if _, ok := specs["testformat1"]; !ok {
		t.Errorf("didn't load correct spec name")
	}
}

func TestLoad(t *testing.T) {
	spec, err := load("./fixtures/testformat1.csv")
	if err != nil {
		t.Fatal(err)
	}

	if spec.Name != "testformat1" {
		t.Errorf("expected: %#v, got: %#v", "testformat1", spec.Name)
	}

	cols := []struct {
		expectName  string
		expectWidth int
		expectType  Datatype
	}{
		{"name", 10, TextType},
		{"valid", 1, BoolType},
		{"count", 3, IntType},
	}

	if len(cols) != len(spec.Columns) {
		t.Fatalf("expected %d columns, got %d", len(cols), len(spec.Columns))
	}

	for i, col := range cols {
		actualCol := spec.Columns[i]
		if col.expectName != actualCol.Name {
			t.Errorf("cols[%d]: expected: %#v, got: %#v", i, col.expectName, actualCol.Name)
		}
		if col.expectWidth != actualCol.Width {
			t.Errorf("cols[%d]: expected: %#v, got: %#v", i, col.expectWidth, actualCol.Width)
		}
		if col.expectType != actualCol.Datatype {
			t.Errorf("cols[%d]: expected: %#v, got: %#v", i, col.expectType, actualCol.Datatype)
		}
	}
}

func TestLoadColumn(t *testing.T) {
	// TODO
}

func TestCreateSQL(t *testing.T) {
	// TODO
}

// -- utils

func TestMapify(t *testing.T) {
	// TODO
}

func TestStripExtension(t *testing.T) {
	tests := []struct {
		given  string
		expect string
	}{
		{"", ""},
		{"foo", "foo"},
		{"bar.txt", "bar"},
		{"baz.biz.csv", "baz.biz"},
	}

	for i, test := range tests {
		got := stripExtension(test.given)
		if test.expect != got {
			t.Errorf("tests[%d]: expected %#v, got: %#v", i, test.expect, got)
		}
	}
}
