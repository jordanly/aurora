// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package validation

import (
	"context"
	_ "embed"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

//go:embed checkstyle-fixtures.json
var checkstyleFixtureJSON []byte

type checkstyleFixture struct {
	Source     string `json:"source"`
	Count      int    `json:"count"`
	ModernOnly bool   `json:"modernOnly"`
}
type xmlElement struct {
	XMLName    xml.Name
	Attributes []xml.Attr   `xml:",any,attr"`
	Children   []xmlElement `xml:",any"`
}

func (e xmlElement) attribute(name string) string {
	for _, a := range e.Attributes {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}
func module(name string) xmlElement {
	return xmlElement{XMLName: xml.Name{Local: "module"}, Attributes: []xml.Attr{{Name: xml.Name{Local: "name"}, Value: name}}}
}
func property(name, value string) xmlElement {
	return xmlElement{XMLName: xml.Name{Local: "property"}, Attributes: []xml.Attr{{Name: xml.Name{Local: "name"}, Value: name}, {Name: xml.Name{Local: "value"}, Value: value}}}
}
func findModule(e xmlElement, name string) (xmlElement, error) {
	for _, child := range e.Children {
		if child.XMLName.Local == "module" && child.attribute("name") == name {
			return child, nil
		}
	}
	return xmlElement{}, fmt.Errorf("missing Checkstyle module %s", name)
}

func checkstyleConfiguration(source []byte, historical bool) ([]byte, error) {
	var original xmlElement
	if err := xml.Unmarshal(source, &original); err != nil {
		return nil, err
	}
	walkerSource, err := findModule(original, "TreeWalker")
	if err != nil {
		return nil, err
	}
	root, walker := module("Checker"), module("TreeWalker")
	for _, name := range []string{"JavadocVariable", "ImportOrder", "FinalClass", "EqualsHashCode"} {
		selected, err := findModule(walkerSource, name)
		if err != nil {
			return nil, err
		}
		if historical {
			if name == "JavadocVariable" {
				selected = module(name)
				selected.Children = []xmlElement{property("excludeScope", "private")}
			} else {
				children := []xmlElement{}
				for _, child := range selected.Children {
					key := child.attribute("name")
					if key != "staticGroups" && key != "separatedStaticGroups" && key != "sortStaticImportsAlphabetically" {
						children = append(children, child)
					}
				}
				selected.Children = children
			}
		}
		walker.Children = append(walker.Children, selected)
	}
	if !historical {
		for _, child := range walkerSource.Children {
			if child.XMLName.Local == "module" && child.attribute("name") == "SuppressionXpathSingleFilter" {
				walker.Children = append(walker.Children, child)
			}
		}
	}
	root.Children = append(root.Children, walker)
	if !historical {
		filter, err := findModule(original, "SuppressionSingleFilter")
		if err != nil {
			return nil, err
		}
		root.Children = append(root.Children, filter)
	}
	data, err := xml.Marshal(root)
	if err != nil {
		return nil, err
	}
	header := `<?xml version="1.0"?>` + "\n" + `<!DOCTYPE module PUBLIC "-//Puppy Crawl//DTD Check Configuration 1.3//EN" "http://www.puppycrawl.com/dtds/configuration_1_3.dtd">` + "\n"
	return append([]byte(header), data...), nil
}

func checkstyleViolations(report []byte) (map[string]int, error) {
	var parsed struct {
		Files []struct {
			Name   string       `xml:"name,attr"`
			Errors []xmlElement `xml:"error"`
		} `xml:"file"`
	}
	if err := xml.Unmarshal(report, &parsed); err != nil {
		return nil, err
	}
	found := map[string]int{}
	for _, file := range parsed.Files {
		name := strings.TrimSuffix(filepath.Base(file.Name), filepath.Ext(file.Name))
		if _, exists := found[name]; exists {
			return nil, fmt.Errorf("duplicate Checkstyle report file %s", name)
		}
		found[name] = len(file.Errors)
	}
	return found, nil
}

func checkstyleMigration(ctx context.Context, root string, args []string) error {
	flags, positional, err := options(args, "--java", "--old-classpath", "--new-classpath", "--receipt")
	if err != nil {
		return err
	}
	if len(positional) > 0 {
		return usage("unexpected positional arguments")
	}
	for _, name := range []string{"--java", "--old-classpath", "--new-classpath", "--receipt"} {
		if flags[name] == "" {
			return usage("checkstyle-migration requires " + name)
		}
	}
	var fixtures map[string]checkstyleFixture
	if err := json.Unmarshal(checkstyleFixtureJSON, &fixtures); err != nil {
		return err
	}
	source, err := os.ReadFile(filepath.Join(root, "config/checkstyle/checkstyle.xml"))
	if err != nil {
		return err
	}
	directory, err := os.MkdirTemp("", "aurora-checkstyle-migration-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	names := []string{}
	for name, fixture := range fixtures {
		names = append(names, name)
		if err := os.WriteFile(filepath.Join(directory, name+".java"), []byte(fixture.Source), 0600); err != nil {
			return err
		}
	}
	sort.Strings(names)
	receipt := map[string]any{}
	for _, version := range []struct {
		label, classpath string
		historical       bool
	}{{"7.3", flags["--old-classpath"], true}, {"14.1.0", flags["--new-classpath"], false}} {
		configuration, err := checkstyleConfiguration(source, version.historical)
		if err != nil {
			return err
		}
		config := filepath.Join(directory, "config-"+version.label+".xml")
		if err := os.WriteFile(config, configuration, 0600); err != nil {
			return err
		}
		output := filepath.Join(directory, "result-"+version.label+".xml")
		command := []string{flags["--java"], "-cp", version.classpath, "com.puppycrawl.tools.checkstyle.Main", "-c", config, "-f", "xml", "-o", output}
		expected := map[string]int{}
		total := 0
		for _, name := range names {
			fixture := fixtures[name]
			if version.historical && fixture.ModernOnly {
				continue
			}
			command = append(command, filepath.Join(directory, name+".java"))
			expected[name] = fixture.Count
			total += fixture.Count
		}
		result, err := runCaptured(ctx, command, os.Environ(), root, 60*time.Second)
		if err != nil {
			return err
		}
		report, err := os.ReadFile(output)
		if err != nil {
			return fmt.Errorf("%s: no Checkstyle report: %v; %s", version.label, err, result["output"])
		}
		found, err := checkstyleViolations(report)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(found, expected) || result["exitCode"] != total {
			return fmt.Errorf("%s: violations %v; exit=%v; %s%s", version.label, found, result["exitCode"], report, result["output"])
		}
		receipt[version.label] = map[string]any{"exitCode": result["exitCode"], "violations": found}
	}
	if err := writeJSON(flags["--receipt"], receipt); err != nil {
		return err
	}
	fmt.Println("Both Checkstyle versions preserve all 18 shared fixture contracts; 3 additional record contracts passed.")
	return nil
}
