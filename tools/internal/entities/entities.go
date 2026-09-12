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

// Package entities generates Aurora's historical immutable Thrift wrappers.
// It intentionally accepts the same restricted, declaration-ordered Thrift
// subset as the original generator; it is not a replacement Thrift compiler.
package entities

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type dataType struct {
	name, pkg, kind, boxed string
	immutable              bool
	params                 []*dataType
	fields                 []field
	values                 []string
}

func (t *dataType) absoluteName() string {
	if t.pkg != "" {
		return t.pkg + "." + t.name
	}
	return t.name
}

func (t *dataType) codegenName() string {
	if t.isStruct() {
		return "I" + t.name
	}
	return t.name
}

func (t *dataType) isStruct() bool {
	return t.kind == "struct" || t.kind == "union" || t.kind == "enum"
}

func (t *dataType) paramNames() string {
	var names []string
	for _, param := range t.params {
		switch {
		case param.isStruct() && !param.immutable:
			names = append(names, param.codegenName())
		case param.kind == "primitive":
			names = append(names, param.boxed)
		default:
			names = append(names, param.name)
		}
	}
	return strings.Join(names, ", ")
}

type field struct {
	typeOf *dataType
	name   string
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	return strings.ToUpper(string(runes[0])) + string(runes[1:])
}

func (f field) accessor() string {
	prefix := "get"
	if f.typeOf.name == "boolean" {
		prefix = "is"
	}
	return prefix + capitalize(f.name)
}

func (f field) isSet() string { return "isSet" + capitalize(f.name) }

var thriftTypes = map[string]*dataType{
	"bool":   {name: "boolean", boxed: "Boolean", kind: "primitive", immutable: true},
	"i32":    {name: "int", boxed: "Integer", kind: "primitive", immutable: true},
	"i64":    {name: "long", boxed: "Long", kind: "primitive", immutable: true},
	"double": {name: "double", boxed: "Double", kind: "primitive", immutable: true},
	"string": {name: "String", boxed: "String", kind: "primitive", immutable: true},
	"binary": {name: "byte[]", boxed: "byte[]", kind: "primitive", immutable: true},
	"list":   {name: "List"},
	"set":    {name: "Set"},
	"map":    {name: "Map"},
}

var (
	namespaceRE = regexp.MustCompile(`namespace\s+(\w+)\s+([^\s]+)`)
	structRE    = regexp.MustCompile(`(enum|struct|union)\s+(\w+)\s+{([^}]+)}`)
	fieldRE     = regexp.MustCompile(`\s*\d+:\s+(?:(?:required|optional)\s+)?(\w+)(?:<([^>]+)>)?\s+(\w+).*`)
	enumValueRE = regexp.MustCompile(`\s*(\w+)\s*=\s*\d+,?`)
	serviceRE   = regexp.MustCompile(`service (\w+)\s+(?:extends\s+(\w+)\s+)?{([^}]+)}`)
	methodRE    = regexp.MustCompile(`\s*(\w+)\s+(\w+)\(([^)]*)\)`)
	parameterRE = regexp.MustCompile(`\d+:\s+(\w+)(?:<([^>]+)>)?\s+(?:\w+)`)
	upperRE     = regexp.MustCompile(`([A-Z])`)
	variableRE  = regexp.MustCompile(`%\((\w+)\)s`)
)

func parseStructs(source string) ([]*dataType, error) {
	namespace := ""
	for _, match := range namespaceRE.FindAllStringSubmatch(source, -1) {
		if match[1] == "java" {
			namespace = match[2]
		}
	}
	var structures []*dataType
	resolve := func(name string) (*dataType, error) {
		if known, ok := thriftTypes[name]; ok {
			return known, nil
		}
		for _, previous := range structures {
			if previous.name == name {
				return previous, nil
			}
		}
		return nil, fmt.Errorf("unknown or forward-referenced type %q", name)
	}
	for _, match := range structRE.FindAllStringSubmatch(source, -1) {
		t := &dataType{name: match[2], pkg: namespace, kind: match[1], immutable: match[1] == "enum"}
		if t.kind == "enum" {
			for _, value := range enumValueRE.FindAllStringSubmatch(match[3], -1) {
				t.values = append(t.values, value[1])
			}
		} else {
			for _, definition := range fieldRE.FindAllStringSubmatch(match[3], -1) {
				var ft *dataType
				if definition[2] != "" {
					ft = &dataType{name: capitalize(strings.ToLower(definition[1])), kind: "parameterized"}
					for _, param := range strings.Split(strings.ReplaceAll(definition[2], " ", ""), ",") {
						resolved, err := resolve(param)
						if err != nil {
							return nil, fmt.Errorf("%s.%s: %w", t.name, definition[3], err)
						}
						ft.params = append(ft.params, resolved)
					}
				} else {
					var err error
					ft, err = resolve(definition[1])
					if err != nil {
						return nil, fmt.Errorf("%s.%s: %w", t.name, definition[3], err)
					}
				}
				t.fields = append(t.fields, field{ft, definition[3]})
			}
		}
		structures = append(structures, t)
	}
	return structures, nil
}

type method struct {
	name       string
	parameters []string
}

type service struct {
	name, parent string
	methods      []method
}

func parseServices(source string) []service {
	var services []service
	for _, definition := range serviceRE.FindAllStringSubmatch(source, -1) {
		s := service{name: definition[1], parent: definition[2]}
		for _, call := range methodRE.FindAllStringSubmatch(definition[3], -1) {
			m := method{name: call[2]}
			for _, param := range parameterRE.FindAllStringSubmatch(call[3], -1) {
				m.parameters = append(m.parameters, param[1])
			}
			s.methods = append(s.methods, m)
		}
		services = append(services, s)
	}
	return services
}

// substitute performs one pass: generated values are never interpreted as templates.
func substitute(template string, values map[string]string) string {
	return variableRE.ReplaceAllStringFunc(template, func(variable string) string {
		key := variable[2 : len(variable)-2]
		value, ok := values[key]
		if !ok {
			panic("missing internal template value: " + key)
		}
		return value
	})
}

type generatedCode struct {
	name, wrapped                       string
	imports                             map[string]bool
	fields, accessors, assignments      []string
	toString, equals, hash, constructor string
}

func (c *generatedCode) addImport(name string) { c.imports[name] = true }

func (c *generatedCode) dump() string {
	var importGroups []string
	for _, prefix := range []string{"java", "com", "net", "org", "com.twitter"} {
		var group []string
		for imported := range c.imports {
			if strings.HasPrefix(imported, prefix) &&
				(prefix != "com" || !strings.HasPrefix(imported, "com.twitter")) {
				group = append(group, "import "+imported+";")
			}
		}
		sort.Strings(group)
		if len(group) > 0 {
			importGroups = append(importGroups, strings.Join(group, "\n"))
		}
	}
	fields, assignments := "", ""
	if len(c.fields) > 0 {
		fields = "  " + strings.Join(c.fields, "\n  ") + "\n"
	}
	if len(c.assignments) > 0 {
		assignments = "\n    " + strings.Join(c.assignments, "\n    ")
	}
	return substitute(classTemplate, map[string]string{
		"package": packageName, "name": c.name, "wrapped": c.wrapped,
		"imports": strings.Join(importGroups, "\n\n"), "accessors": strings.Join(c.accessors, "\n\n"),
		"fields": fields, "assignments": assignments, "to_string": c.toString,
		"equals": c.equals, "hashcode": c.hash, "copy_constructor": c.constructor,
	}) + "\n"
}

func generateStructField(c *generatedCode, f field) (string, error) {
	t := f.typeOf
	fieldType := t.codegenName()
	assignment := simpleAssignment
	args := map[string]string{"field": f.name, "fn_name": f.accessor()}
	builderAssignment := f.name
	accessorType := fieldType
	if t.immutable {
		accessorType = t.name
	} else if t.kind == "parameterized" {
		for _, param := range t.params {
			if param.kind == "enum" {
				c.addImport(param.absoluteName())
			}
		}
		fieldType = "Immutable" + t.name + "<" + t.paramNames() + ">"
		accessorType = fieldType
	}
	c.accessors = append(c.accessors, substitute(fieldTemplate, map[string]string{
		"type": accessorType, "fn_name": f.accessor(), "field": f.name,
	}))
	if t.isStruct() {
		if t.kind == "enum" {
			fieldType = t.name
			c.addImport(t.absoluteName())
		}
		if !t.immutable {
			assignment = structAssignment
			args = map[string]string{"field": f.name, "fn_name": f.accessor(),
				"isset": f.isSet(), "type": t.codegenName()}
			builderAssignment = f.name + ".newBuilder()"
		}
	} else if t.kind == "parameterized" {
		if t.name != "List" && t.name != "Map" && t.name != "Set" {
			return "", fmt.Errorf("unrecognized collection type %s", t.name)
		}
		c.addImport("com.google.common.collect.Immutable" + t.name)
		allImmutable := true
		for _, param := range t.params {
			allImmutable = allImmutable && param.immutable
		}
		if allImmutable {
			assignment = immutableCollectionAssignment
		} else if len(t.params) == 1 {
			assignment = structCollectionFieldAssignment
			builderAssignment = t.params[0].codegenName() + ".toMutableBuilders" + t.name + "(" + f.name + ")"
		} else {
			return "", fmt.Errorf("unable to generate accessor field for %s", f.name)
		}
		args = map[string]string{"collection": t.name, "field": f.name,
			"fn_name": f.accessor(), "isset": f.isSet(), "params": t.paramNames()}
	}
	c.fields = append(c.fields, substitute(fieldDeclaration, map[string]string{
		"field": f.name, "type": fieldType,
	}))
	nullable := t.name == "String" || (t.kind != "primitive" && t.kind != "parameterized")
	if nullable {
		c.accessors = append(c.accessors, substitute(fieldTemplate, map[string]string{
			"type": "boolean", "fn_name": f.isSet(), "field": f.name + " != null",
		}))
		builderAssignment = f.name + " == null ? null : " + builderAssignment
	}
	c.assignments = append(c.assignments, substitute(assignment, args))
	return ".set" + capitalize(f.name) + "(" + builderAssignment + ")", nil
}

func upperSnake(name string) string { return strings.ToUpper(upperRE.ReplaceAllString(name, "_$1")) }

func unionSwitch(cases []string, by, failure string) string {
	return substitute(unionFieldSwitch, map[string]string{
		"cases": strings.Join(cases, "\n      "), "switch_by": by, "error": failure,
	})
}

func generateJava(t *dataType) (string, error) {
	c := &generatedCode{name: t.codegenName(), wrapped: t.name, imports: map[string]bool{}}
	for _, imported := range []string{"java.util.Objects", "java.util.List", "java.util.Set",
		"com.google.common.base.MoreObjects", "com.google.common.collect.ImmutableList",
		"com.google.common.collect.ImmutableSet", "com.google.common.collect.FluentIterable",
		"com.google.common.collect.Iterables", "com.google.common.collect.Lists",
		"com.google.common.collect.Sets", t.absoluteName()} {
		c.addImport(imported)
	}
	if t.kind == "union" {
		var assignments, copies, copies2 []string
		for _, f := range t.fields {
			enumValue := t.name + "._Fields." + upperSnake(f.name)
			c.accessors = append(c.accessors, substitute(fieldTemplate, map[string]string{
				"type": "boolean", "fn_name": f.isSet(), "field": "setField == " + enumValue,
			}), substitute(unionFieldTemplate, map[string]string{
				"type": f.typeOf.codegenName(), "fn_name": f.accessor(), "enum_value": enumValue,
			}))
			if !f.typeOf.immutable && !f.typeOf.isStruct() {
				return "", fmt.Errorf("unrecognized union type %s", f.typeOf.name)
			}
			assignment := "value = wrapped." + f.accessor() + "();\nbreak;"
			copyBody := "return new " + t.name + "(setField, " + f.accessor() + "());"
			cast := f.typeOf.codegenName()
			if !f.typeOf.immutable {
				assignment = "value = " + f.typeOf.codegenName() + ".build(wrapped." + f.accessor() + "());\nbreak;"
				copyBody = "return new " + t.name + "(setField, " + f.accessor() + "().newBuilder());"
				c.addImport("org.apache.aurora.gen." + f.typeOf.name)
				cast = f.typeOf.name
			}
			caseFor := func(body string) string {
				return substitute(unionSwitchCase, map[string]string{"case": upperSnake(f.name), "body": body})
			}
			assignments = append(assignments, caseFor(assignment))
			copies = append(copies, caseFor(copyBody))
			copies2 = append(copies2, caseFor("return "+t.name+"."+f.name+"(("+cast+") value);"))
		}
		setFieldType := t.name + "._Fields"
		c.accessors = append(c.accessors, substitute(fieldTemplate, map[string]string{
			"type": setFieldType, "fn_name": "getSetField", "field": "setField",
		}))
		c.fields = append(c.fields, "private final "+setFieldType+" setField;", "private final Object value;")
		c.assignments = append(c.assignments, "this.setField = wrapped.getSetField();",
			unionSwitch(assignments, "getSetField()", unionDefaultError))
		c.constructor = unionSwitch(copies, "getSetField()", unionDefaultError)
		c.accessors = append(c.accessors, unionValueAccessor,
			substitute(unionCopyConstructor2, map[string]string{"wrapped": t.name,
				"body": unionSwitch(copies2, setFieldType+".findByThriftId(id)",
					`throw new RuntimeException("Unrecognized id " + id)`)}))
		c.toString = `.add("setField", setField).add("value", value)`
		c.equals = "Objects.equals(setField, other.setField) && Objects.equals(value, other.value)"
		c.hash = "setField, value"
	} else {
		var builders, names, equality, toString []string
		for _, f := range t.fields {
			builder, err := generateStructField(c, f)
			if err != nil {
				return "", fmt.Errorf("%s: %w", t.name, err)
			}
			builders = append(builders, builder)
			names = append(names, f.name)
			equality = append(equality, "Objects.equals("+f.name+", other."+f.name+")")
			toString = append(toString, ".add(\""+f.name+"\", "+f.name+")")
		}
		c.constructor = "return new " + t.name + "()\n        " + strings.Join(builders, "\n        ") + ";"
		c.toString = "\n        " + strings.Join(toString, "\n        ")
		c.equals = strings.Join(equality, "\n        && ")
		c.hash = "\n          " + strings.Join(names, ",\n          ")
	}
	if len(t.fields) == 0 {
		c.equals = "true"
	}
	return c.dump(), nil
}

func generateMetadata(services []service) (string, error) {
	lookup := func(name string) (service, error) {
		for _, s := range services {
			if s.name == name {
				return s, nil
			}
		}
		return service{}, fmt.Errorf("missing service %q", name)
	}
	current, err := lookup("AuroraAdmin")
	if err != nil {
		return "", err
	}
	var methods []string
	seen := map[string]bool{}
	for {
		if seen[current.name] {
			return "", fmt.Errorf("cyclic service inheritance at %s", current.name)
		}
		seen[current.name] = true
		for _, m := range current.methods {
			var params []string
			for _, name := range m.parameters {
				if known, ok := thriftTypes[name]; ok {
					name = known.name
					if known.kind == "primitive" {
						name = known.boxed
					}
				}
				params = append(params, name+".class,")
			}
			spacing := "\n                  "
			parameters := strings.Join(params, spacing)
			if len(params) > 0 {
				parameters = spacing + parameters
			}
			methods = append(methods, substitute(methodMetadataTemplate, map[string]string{
				"name": m.name, "params": parameters,
			}))
		}
		if current.parent == "" {
			break
		}
		current, err = lookup(current.parent)
		if err != nil {
			return "", err
		}
	}
	return substitute(serviceMetadataTemplate, map[string]string{"package": packageName,
		"methods": strings.Join(methods, "\n          "), "name": "AuroraAdmin"}) + "\n", nil
}

// Generate writes deterministic immutable wrappers and AuroraAdmin method metadata.
// Schemas without services (including included storage schemas) intentionally emit
// nothing. Includes are handled by the actual Thrift compiler, not this generator.
func Generate(schema, javaOut, resourcesOut string) error {
	contents, err := os.ReadFile(schema)
	if err != nil {
		return err
	}
	services := parseServices(string(contents))
	if len(services) == 0 {
		return nil
	}
	structures, err := parseStructs(string(contents))
	if err != nil {
		return err
	}
	// Complete semantic validation before creating partial output.
	outputs := map[string]string{}
	for _, structure := range structures {
		if structure.kind == "enum" {
			continue
		}
		generated, err := generateJava(structure)
		if err != nil {
			return err
		}
		outputs[structure.codegenName()+".java"] = generated
	}
	metadata, err := generateMetadata(services)
	if err != nil {
		return err
	}
	outputs["AuroraAdminMetadata.java"] = metadata
	packagePath := filepath.FromSlash(strings.ReplaceAll(packageName, ".", "/"))
	packageDir := filepath.Join(javaOut, packagePath)
	for _, directory := range []string{packageDir,
		filepath.Join(resourcesOut, packagePath, "help", "method"),
		filepath.Join(resourcesOut, packagePath, "help", "type")} {
		if err := os.MkdirAll(directory, 0755); err != nil {
			return err
		}
	}
	names := make([]string, 0, len(outputs))
	for name := range outputs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(packageDir, name), []byte(outputs[name]), 0644); err != nil {
			return err
		}
	}
	return nil
}
