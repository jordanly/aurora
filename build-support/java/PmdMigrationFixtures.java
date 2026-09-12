/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import net.sourceforge.pmd.PMDConfiguration;
import net.sourceforge.pmd.PmdAnalysis;
import net.sourceforge.pmd.lang.document.FileId;

/** Executes positive and negative migration fixtures using the production custom rules. */
public final class PmdMigrationFixtures {
  private PmdMigrationFixtures() { }

  public static void main(String[] args) throws Exception {
    Class.forName("org.junit.Assert");
    Class.forName("com.google.common.collect.ImmutableSet");
    Class.forName("org.apache.shiro.config.Ini");
    String timed = "config/pmd/timed.xml";
    String compatibility = "config/pmd/compatibility.xml";
    String method = "TimedAnnotationNonOverridableMethod";
    String type = "TimedAnnotationNonOverridableClass";
    check(timed, "publicMethod", "class Example { @Timed public void run() {} }", Map.of());
    check(timed, "protectedMethod", "class Example { @Timed protected void run() {} }", Map.of());
    check(timed, "packageMethod", "class Example { @Timed void run() {} }", Map.of());
    check(timed, "privateMethod", "class Example { @Timed private void run() {} }", Map.of(method, 1));
    check(timed, "staticMethod", "class Example { @Timed static void run() {} }", Map.of(method, 1));
    check(timed, "finalMethod", "class Example { @Timed final void run() {} }", Map.of(method, 1));
    check(timed, "finalClass", "final class Example { @Timed void run() {} }", Map.of(type, 1));
    check(timed, "both", "final class Example { @Timed private void run() {} }",
        Map.of(method, 1, type, 1));
    check(timed, "privateClass", "class Outer { private class Example { @Timed void run() {} } }",
        Map.of(type, 1));
    check(timed, "protectedClass", "class Outer { protected class Example { @Timed void run() {} } }",
        Map.of(type, 1));
    check(timed, "parameter", "final class Example { private void run(@Timed int value) {} }", Map.of());
    check(timed, "nestedClass", "final class Outer { class Inner { @Timed void run() {} } }", Map.of());
    check(timed, "localClass", "class Outer { private void run() { class Inner { @Timed void run() {} } } }", Map.of());
    check(timed, "annotationValue", "class Example { @Container(@Timed) private void run() {} }", Map.of());
    check(timed, "unannotated", "final class Example { private void run() {} }", Map.of());
    check(timed, "qualifiedAnnotation", "class Example { @example.Timed private void run() {} }", Map.of());
    check(timed, "constructor", "final class Example { @Timed Example() {} }", Map.of(type, 1));
    check(compatibility, "arrayNull", "class Example { int[] run() { return null; } }",
        Map.of("ReturnEmptyArrayRatherThanNull", 1));
    check(compatibility, "arrayEmpty", "class Example { int[] run() { return new int[0]; } }", Map.of());
    check(compatibility, "objectNull", "class Example { Object run() { return null; } }", Map.of());
    check(compatibility, "nestedArrayNull", "class Example { int[] run(boolean b) { if (b) { return null; } return new int[0]; } }", Map.of());
    check(compatibility, "abstractBad", "abstract class Example { }", Map.of("AbstractNaming", 1));
    check(compatibility, "abstractGood", "abstract class AbstractExample { }", Map.of());
    check(compatibility, "concreteBad", "class AbstractExample { }", Map.of("AbstractNaming", 1));
    check(compatibility, "interfaceGood", "interface Example { }", Map.of());
    check("category/java/codestyle.xml/ClassNamingConventions", "defaultAbstractName",
        "abstract class Example { }", Map.of());
    check("category/java/codestyle.xml/LocalVariableNamingConventions", "misleadingLocal",
        "class Example { void run() { int m_value = 1; } }",
        Map.of("LocalVariableNamingConventions", 1));
    check("category/java/codestyle.xml/FormalParameterNamingConventions", "misleadingParameter",
        "class Example { void run(int m_value) { } }",
        Map.of("FormalParameterNamingConventions", 1));
    check("category/java/codestyle.xml/FieldNamingConventions", "suspiciousConstant",
        "class Example { int VALUE = 1; }", Map.of("FieldNamingConventions", 1));
    check("category/java/codestyle.xml/UnnecessaryModifier", "interfaceModifiers",
        "interface Example { public abstract void run(); }", Map.of("UnnecessaryModifier", 1));
    check("category/java/bestpractices.xml/ConstantsInInterface", "constantsInterface",
        "interface Example { int VALUE = 1; }", Map.of("ConstantsInInterface", 1));
    check("category/java/bestpractices.xml/ConstantsInInterface", "interfaceWithMethod",
        "interface Example { int VALUE = 1; void run(); }", Map.of());
    check("category/java/design.xml/InstantiableUtilityClass", "utilityClass",
        "class Example { public static void run() {} }", Map.of("InstantiableUtilityClass", 1));
    for (String ruleset : java.util.List.of("config/pmd/main.xml/CloseResource",
        "config/pmd/test.xml/CloseResource")) {
      check(ruleset, "connectionLeak", "import java.sql.*; class Example { void run() throws Exception { Connection c = DriverManager.getConnection(\"url\"); } }", Map.of("CloseResource", 1));
      check(ruleset, "statementLeak", "import java.sql.*; class Example { void run(Connection c) throws Exception { Statement s = c.createStatement(); } }", Map.of("CloseResource", 1));
      check(ruleset, "resultSetLeak", "import java.sql.*; class Example { void run(Statement s) throws Exception { ResultSet r = s.executeQuery(\"sql\"); } }", Map.of("CloseResource", 1));
      check(ruleset, "closedConnection", "import java.sql.*; class Example { void run() throws Exception { try (Connection c = DriverManager.getConnection(\"url\")) {} } }", Map.of());
      check(ruleset, "executorScope", "import java.util.concurrent.*; class Example { void run() { ExecutorService e = Executors.newSingleThreadExecutor(); } }", Map.of());
      check(ruleset, "closeableScope", "class Example { void run() { AutoCloseable c = () -> {}; } }", Map.of());
    }
    String equality = "config/pmd/common.xml/OverrideBothEqualsAndHashcode";
    check(equality, "recordHash", "record Example(int value) { public int hashCode() { return value; } }", Map.of());
    check(equality, "classHash", "class Example { public int hashCode() { return 1; } }", Map.of("OverrideBothEqualsAndHashcode", 1));
    check(equality, "recordEquals", "record Example(int value) { public boolean equals(Object o) { return true; } }", Map.of("OverrideBothEqualsAndHashcode", 1));
    check(equality, "nestedClassHash", "record Example(int value) { static class Inner { public int hashCode() { return 1; } } }", Map.of("OverrideBothEqualsAndHashcode", 1));
    String resultSet = "config/pmd/common.xml/CheckResultSet";
    check(resultSet, "uncheckedRows", "import java.sql.*; class Example { void run(ResultSet r) throws Exception { r.next(); } }", Map.of("CheckResultSet", 1));
    check(resultSet, "assertedRows", "import java.sql.*; import static org.junit.Assert.assertTrue; class Example { void run(ResultSet r) throws Exception { assertTrue(r.next()); } }", Map.of());
    check(resultSet, "assertedRowsMessage", "import java.sql.*; class Example { void run(ResultSet r) throws Exception { org.junit.Assert.assertTrue(\"row exists\", r.next()); } }", Map.of());
    check(resultSet, "arbitraryConsumer", "import java.sql.*; class Example { void consume(boolean value) {} void run(ResultSet r) throws Exception { consume(r.next()); } }", Map.of("CheckResultSet", 1));
    check(resultSet, "fakeAssertion", "import java.sql.*; class Example { void assertTrue(boolean value) {} void run(ResultSet r) throws Exception { assertTrue(r.next()); } }", Map.of("CheckResultSet", 1));
    check(resultSet, "nestedUncheckedCall", "import java.sql.*; class Example { boolean consume(boolean value) { return true; } void run(ResultSet r) throws Exception { org.junit.Assert.assertTrue(consume(r.next())); } }", Map.of("CheckResultSet", 1));
    String privateConstructors = "config/pmd/compatibility.xml/ClassWithOnlyPrivateConstructorsShouldBeFinal";
    check(privateConstructors, "explicitPrivateCtor", "class Example { private Example() {} }", Map.of("ClassWithOnlyPrivateConstructorsShouldBeFinal", 1));
    check(privateConstructors, "implicitCtor", "class Example {}", Map.of());
    check(privateConstructors, "implicitNestedPrivateCtor", "class Example { private static class Inner {} }", Map.of());
    check(privateConstructors, "explicitNestedPrivateCtor", "class Example { private static class Inner { private Inner() {} } }", Map.of());
    check(privateConstructors, "privateCtorWithNestedClass", "class Example { private Example() {} class Inner {} }", Map.of());
    check(privateConstructors, "multipleTopLevelClasses", "class Example { private Example() {} } class Other {}", Map.of());
    check(privateConstructors, "finalPrivateCtor", "final class Example { private Example() {} }", Map.of());
    String coupling = "config/pmd/test.xml/LooseCoupling";
    for (String collection : java.util.List.of("ArrayList", "LinkedList", "Vector", "HashMap",
        "LinkedHashMap", "TreeMap", "TreeSet", "HashSet", "LinkedHashSet", "Hashtable")) {
      check(coupling, "collectionField" + collection,
          "class Example { java.util." + collection + " value; }", Map.of("LooseCoupling", 1));
      check(coupling, "collectionParameter" + collection,
          "class Example { void run(java.util." + collection + " value) {} }", Map.of("LooseCoupling", 1));
      check(coupling, "collectionResult" + collection,
          "class Example { java.util." + collection + " run() { return null; } }", Map.of("LooseCoupling", 1));
    }
    check(coupling, "collectionLocal", "class Example { void run() { java.util.ArrayList value = new java.util.ArrayList(); } }", Map.of());
    check(coupling, "collectionArrayField", "class Example { java.util.ArrayList[] value; }", Map.of("LooseCoupling", 1));
    check(coupling, "collectionInterface", "class Example { java.util.List value; }", Map.of());
    check(coupling, "collectionSubclass", "class Example { static class Specialized extends java.util.ArrayList {} Specialized value; }", Map.of());
    for (String collection : java.util.List.of("com.google.common.collect.ImmutableSet",
        "com.google.common.collect.ImmutableList", "com.google.common.collect.ImmutableMap",
        "org.apache.shiro.config.Ini", "org.apache.shiro.config.Ini.Section")) {
      String source = "class Example { " + collection + " value; }";
      check("category/java/bestpractices.xml/LooseCoupling", "expanded" + collection.replace('.', '_'),
          source, Map.of("LooseCoupling", 1));
      check(coupling, "historical" + collection.replace('.', '_'), source, Map.of());
    }
    verifyRulesets();
    System.out.println("PMD migration fixtures passed: 106 exact positive/negative cases and ruleset wiring.");
  }

  private static void check(String ruleset, String name, String source, Map<String, Integer> expected) {
    PMDConfiguration configuration = new PMDConfiguration();
    configuration.setIgnoreIncrementalAnalysis(true);
    configuration.setThreads(1);
    configuration.setAuxClasspath(System.getProperty("java.class.path"));
    try (PmdAnalysis analysis = PmdAnalysis.create(configuration)) {
      analysis.addRuleSet(analysis.newRuleSetLoader().loadFromResource(ruleset));
      if (!analysis.files().addSourceFile(FileId.fromPath(Path.of(name + ".java")), source)) {
        throw new AssertionError("Fixture not added: " + name);
      }
      var report = analysis.performAnalysisAndCollectReport();
      if (!report.getProcessingErrors().isEmpty() || !report.getConfigurationErrors().isEmpty()) {
        throw new AssertionError(name + " analysis failed: " + report.getProcessingErrors().stream().map(e -> e.getDetail()).toList()
            + report.getConfigurationErrors());
      }
      Map<String, Integer> actual = new TreeMap<>();
      report.getViolations().forEach(v -> actual.merge(v.getRule().getName(), 1, Integer::sum));
      if (!actual.equals(expected)) {
        throw new AssertionError(name + ": expected " + expected + " but got " + actual);
      }
    }
  }

  private static void verifyRulesets() {
    try (PmdAnalysis analysis = PmdAnalysis.create(new PMDConfiguration())) {
      var loader = analysis.newRuleSetLoader();
      var common = loader.loadFromResource("config/pmd/common.xml");
      var main = loader.loadFromResource("config/pmd/main.xml");
      var test = loader.loadFromResource("config/pmd/test.xml");
      require(common.getRuleByName("AbstractNaming") == null, "common excludes AbstractNaming");
      require(test.getRuleByName("AbstractNaming") != null, "test preserves AbstractNaming");
      require(test.getRuleByName("SignatureDeclareThrowsException") == null,
          "test excludes SignatureDeclareThrowsException");
      require(common.getRuleByName("UnnecessaryModifier") != null, "modifier coverage");
      for (var rules : java.util.List.of(main, test)) {
        require(rules.getRuleByName("ReturnEmptyArrayRatherThanNull") != null, "array coverage");
        require(rules.getRuleByName("ConstantsInInterface") != null, "constant-interface coverage");
      }
      require(main.getRuleByName("InstantiableUtilityClass") != null, "main utility coverage");
      require(test.getRuleByName("InstantiableUtilityClass") == null, "test utility exclusion");
      require(main.getRuleByName("TimedAnnotationNonOverridableMethod") != null, "Timed method wiring");
      require(main.getRuleByName("TimedAnnotationNonOverridableClass") != null, "Timed class wiring");
    }
  }

  private static void require(boolean condition, String description) {
    if (!condition) {
      throw new AssertionError(description);
    }
  }
}
