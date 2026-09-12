// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entities

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const fixture = `namespace java org.apache.aurora.gen
enum Color { RED = 1, BLUE = 2 }
struct Child { 1: string name }
struct Empty { }
struct Example {
  1: bool enabled
  2: i64 count
  3: Child child
  4: list<Child> children
  5: set<Color> colors
  6: map<string, string> lookup
}
union Choice {
  1: string text
  2: Child child
}
service AuroraReadOnly { Example get(1: string name) }
service AuroraAdmin extends AuroraReadOnly { void put(1: Example value) }
`

func structure(t *testing.T, name string) *dataType {
	t.Helper()
	types, err := parseStructs(fixture)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range types {
		if kind.name == name {
			return kind
		}
	}
	t.Fatalf("missing fixture structure %s", name)
	return nil
}

func render(t *testing.T, name string) string {
	t.Helper()
	output, err := generateJava(structure(t, name))
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func contains(t *testing.T, output string, fragments ...string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(output, fragment) {
			t.Errorf("missing %q in generated source", fragment)
		}
	}
}

func TestFieldsAreReusableAndRepeatable(t *testing.T) {
	typeOf := structure(t, "Example")
	if len(typeOf.fields) != 6 {
		t.Fatal(typeOf.fields)
	}
	first, err := generateJava(typeOf)
	if err != nil {
		t.Fatal(err)
	}
	second, err := generateJava(typeOf)
	if err != nil || first != second || len(typeOf.fields) != 6 {
		t.Fatal("generation mutated reusable fields", err)
	}
}

func TestEqualityHashAndStringIncludeEveryField(t *testing.T) {
	output := render(t, "Example")
	for _, name := range []string{"enabled", "count", "child", "children", "colors", "lookup"} {
		contains(t, output, "Objects.equals("+name+", other."+name+")",
			".add(\""+name+"\", "+name+")")
	}
	contains(t, output, "Objects.hash(\n          enabled,\n          count,")
}

func TestEmptyStructEquality(t *testing.T) {
	output := render(t, "Empty")
	contains(t, output, "return true;")
	if strings.Contains(output, "return ;") {
		t.Fatal("invalid empty equality")
	}
}

func TestImmutableContainersAndNestedStructs(t *testing.T) {
	contains(t, render(t, "Example"), "ImmutableList<IChild>", "ImmutableSet<Color>",
		"ImmutableMap<String, String>", "IChild.build(wrapped.getChild())", "child.newBuilder()",
		"import org.apache.aurora.gen.Color;", "IChild.toMutableBuildersList(children)")
	if !reflect.DeepEqual(structure(t, "Color").values, []string{"RED", "BLUE"}) {
		t.Fatal("enum values changed")
	}
}

func TestUnionDiscriminantAndCopyCases(t *testing.T) {
	contains(t, render(t, "Choice"), "Choice._Fields.TEXT", "Choice._Fields.CHILD",
		"case TEXT:", "case CHILD:", "Choice.child((Child) value)",
		"Objects.equals(setField, other.setField)")
}

func writeSchema(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.thrift")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func generate(t *testing.T, schema string) (string, map[string]string) {
	t.Helper()
	output := t.TempDir()
	if err := Generate(schema, filepath.Join(output, "java"), filepath.Join(output, "resources")); err != nil {
		t.Fatal(err)
	}
	return output, hashes(t, output)
}

func hashes(t *testing.T, output string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.WalkDir(output, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(output, path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		result[filepath.ToSlash(relative)] = hex.EncodeToString(digest[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestMetadataInheritanceSkipsEnumsAndCreatesHelpDirectories(t *testing.T) {
	out, files := generate(t, writeSchema(t, fixture))
	packagePath := filepath.FromSlash(strings.ReplaceAll(packageName, ".", "/"))
	metadata, err := os.ReadFile(filepath.Join(out, "java", packagePath, "AuroraAdminMetadata.java"))
	if err != nil {
		t.Fatal(err)
	}
	contains(t, string(metadata), `"put",`, `"get",`, "Example.class", "String.class")
	if strings.Index(string(metadata), `"put",`) > strings.Index(string(metadata), `"get",`) {
		t.Fatal("child methods must precede inherited methods")
	}
	if len(files) != 5 {
		t.Fatal("must generate four structs and metadata, but no enum", files)
	}
	for _, name := range []string{"type", "method"} {
		entries, err := os.ReadDir(filepath.Join(out, "resources", packagePath, "help", name))
		if err != nil || len(entries) != 0 {
			t.Fatal("historical help directories should be empty", entries, err)
		}
	}
}

func TestNoServicesDoesNotResolveIncludesOrEmitFiles(t *testing.T) {
	_, files := generate(t, writeSchema(t, "include \"missing.thrift\"\nstruct A { 1: Missing value }"))
	if len(files) != 0 {
		t.Fatal(files)
	}
	_, files = generate(t, filepath.Join("..", "..", "..", "api", "src", "main", "thrift",
		"org", "apache", "aurora", "gen", "storage.thrift"))
	if len(files) != 0 {
		t.Fatal(files)
	}
}

func TestAPIMatchesHistoricalByteGoldenAndRepeatedRuns(t *testing.T) {
	goldenBytes, err := os.ReadFile(filepath.Join("testdata", "wrapper-api-sha256.json"))
	if err != nil {
		t.Fatal(err)
	}
	golden := map[string]string{}
	if err := json.Unmarshal(goldenBytes, &golden); err != nil {
		t.Fatal(err)
	}
	schema := filepath.Join("..", "..", "..", "api", "src", "main", "thrift",
		"org", "apache", "aurora", "gen", "api.thrift")
	_, first := generate(t, schema)
	_, second := generate(t, schema)
	if len(first) != 89 || !reflect.DeepEqual(first, second) {
		t.Fatal("unstable output", len(first), len(second))
	}
	for path, expected := range golden {
		if first[path] != expected {
			t.Errorf("byte mismatch for %s: got %s, expected %s", path, first[path], expected)
		}
	}
	if len(first) != len(golden) {
		t.Fatal("unexpected generated files", len(first), len(golden))
	}
}

func TestUnsupportedTypesAndInheritanceFailBeforeWriting(t *testing.T) {
	for name, source := range map[string]string{
		"unknown":       "struct A { 1: Missing value }\nservice AuroraAdmin { void ping() }",
		"forward":       "struct A { 1: B value }\nstruct B { }\nservice AuroraAdmin { void ping() }",
		"recursive":     "struct A { 1: A value }\nservice AuroraAdmin { void ping() }",
		"mutable map":   "struct A { }\nstruct B { 1: map<string, A> value }\nservice AuroraAdmin { void ping() }",
		"unknown list":  "struct A { 1: bag<string> value }\nservice AuroraAdmin { void ping() }",
		"union list":    "union A { 1: list<string> value }\nservice AuroraAdmin { void ping() }",
		"missing admin": "service Other { void ping() }",
		"missing base":  "service AuroraAdmin extends Missing { void ping() }",
		"cyclic base":   "service AuroraAdmin extends Other { void ping() }\nservice Other extends AuroraAdmin { void pong() }",
	} {
		t.Run(name, func(t *testing.T) {
			output := t.TempDir()
			err := Generate(writeSchema(t, source), filepath.Join(output, "java"), filepath.Join(output, "resources"))
			if err == nil {
				t.Fatal("expected unsupported schema to fail")
			}
			if len(hashes(t, output)) != 0 {
				t.Fatal("failed generation wrote partial source")
			}
		})
	}
}

func TestPrimitiveOptionalAndCollectionMetadata(t *testing.T) {
	source := `namespace java org.apache.aurora.gen
struct Numbers {
  1: optional bool yes
  2: required i32 small
  3: double ratio
  4: binary bytes
  5: string name
  6: list<i64> longs
}
service AuroraAdmin { void call(1: bool yes, 2: i32 count, 3: binary data, 4: list<Numbers> values, 5: map<string, i64> names) }
`
	types, err := parseStructs(source)
	if err != nil {
		t.Fatal(err)
	}
	output, err := generateJava(types[0])
	if err != nil {
		t.Fatal(err)
	}
	contains(t, output, "public boolean isYes()", "public int getSmall()", "public double getRatio()",
		"public byte[] getBytes()", "public boolean isSetName()", "ImmutableList<Long>")
	metadata, err := generateMetadata(parseServices(source))
	if err != nil {
		t.Fatal(err)
	}
	contains(t, metadata, "Boolean.class,", "Integer.class,", "byte[].class,", "List.class,", "Map.class,")
}

func TestMissingInputAndUnwritableOutput(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(filepath.Join(dir, "missing.thrift"), dir, dir); err == nil {
		t.Fatal("missing input was ignored")
	}
	file := writeSchema(t, fixture)
	if err := Generate(file, file, dir); err == nil {
		t.Fatal("output under a file was accepted")
	}
}

func TestSchemaRegressionGoldens(t *testing.T) {
	for _, name := range []string{"collections", "parser-boundaries"} {
		t.Run(name, func(t *testing.T) {
			goldenBytes, err := os.ReadFile(filepath.Join("testdata", name+".sha256.json"))
			if err != nil {
				t.Fatal(err)
			}
			golden := map[string]string{}
			if err := json.Unmarshal(goldenBytes, &golden); err != nil {
				t.Fatal(err)
			}
			_, actual := generate(t, filepath.Join("testdata", name+".thrift"))
			if !reflect.DeepEqual(actual, golden) {
				t.Fatalf("historical source bytes differ\nactual: %v\ngolden: %v", actual, golden)
			}
		})
	}
}

func FuzzParserAndRenderer(f *testing.F) {
	f.Add(fixture)
	f.Add("service AuroraAdmin extends AuroraAdmin { void loop() }")
	f.Add("struct A { 1: map<string, A> cycle } service AuroraAdmin { void call() }")
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > 1024*1024 {
			t.Skip()
		}
		structures, err := parseStructs(source)
		if err == nil {
			for _, structure := range structures {
				if structure.kind != "enum" {
					// Unsupported schemas may fail, but must not panic or recurse forever.
					_, _ = generateJava(structure)
				}
			}
		}
		_, _ = generateMetadata(parseServices(source))
	})
}
