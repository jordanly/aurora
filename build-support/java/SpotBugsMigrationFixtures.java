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
import java.util.List;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.filter.Filter;

/** Validates the production exclusion filter using SpotBugs' actual matching implementation. */
public final class SpotBugsMigrationFixtures {
  private static final String EXAMPLE = "migration.fixture.Example";
  private static int checks;

  private SpotBugsMigrationFixtures() { }

  public static void main(String[] args) throws Exception {
    Filter filter = Filter.parseFilter("config/spotbugs/excludeFilter.xml");
    for (String pattern : List.of("EI_EXPOSE_REP", "EI_EXPOSE_REP2",
        "EI_EXPOSE_STATIC_REP2", "MS_EXPOSE_REP")) {
      for (String signature : List.of("[B", "[I", "[[B", "[Ljava/lang/String;",
          "[[Ljava/util/List;", "Ljava/util/Hashtable;", "Ljava/util/Date;",
          "Ljava/sql/Date;", "Ljava/sql/Timestamp;")) {
        check(filter, field(pattern, signature), false);
      }
      for (String signature : List.of("Ljava/util/List;", "Ljava/util/Map;", "Ljava/util/Set;",
          "Lorg/apache/aurora/scheduler/storage/Storage;",
          "Lorg/apache/mesos/v1/Protos$Offer;")) {
        check(filter, field(pattern, signature), true);
      }
      check(filter, field(pattern, "Lmigration/fixture/UnknownMutable;"), false);
      check(filter, new BugInstance(pattern, 2).addClass(EXAMPLE), false);
    }
    for (String pattern : List.of("CT_CONSTRUCTOR_THROW", "AT_NONATOMIC_64BIT_PRIMITIVE",
        "JUA_DONT_ASSERT_INSTANCEOF_IN_TESTS", "DCN_NULLPOINTER_EXCEPTION")) {
      check(filter, new BugInstance(pattern, 2).addClass(EXAMPLE), true);
    }
    for (String pattern : List.of("UL_UNRELEASED_LOCK", "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
        "AT_NONATOMIC_OPERATIONS_ON_SHARED_VARIABLE", "EI_EXPOSE_BUF")) {
      check(filter, field(pattern, "Ljava/util/List;"), false);
    }
    // Existing exclusions remain effective without becoming package-wide suppressions.
    check(filter, new BugInstance("RV_RETURN_VALUE_IGNORED", 2).addClass(EXAMPLE), true);
    check(filter, new BugInstance("UUF_UNUSED_FIELD", 2).addClass("org.apache.aurora.gen.Example"), true);
    check(filter, new BugInstance("NP_ALWAYS_NULL", 2).addClass("org.apache.aurora.gen.Example"), false);
    check(filter, new BugInstance("NP_ALWAYS_NULL", 2).addClass(EXAMPLE), false);
    System.out.println("SpotBugs migration filter fixtures passed: " + checks + " exact cases.");
  }

  private static BugInstance field(String pattern, String signature) {
    return new BugInstance(pattern, 2).addClass(EXAMPLE)
        .addField(EXAMPLE, "value", signature, false);
  }

  private static void check(Filter filter, BugInstance bug, boolean excluded) {
    boolean actual = filter.match(bug);
    if (actual != excluded) {
      throw new AssertionError(bug.getType() + " " + bug.getPrimaryField()
          + ": expected excluded=" + excluded + ", got " + actual);
    }
    checks++;
  }
}
