# JAVA-11: maintained Java 25 build entry point

Step 2 of JAVA-11 is implemented in the working tree. The repository root is
now the maintained development entry point: `./gradlew` invokes the pinned
Linux ARM64 launcher and the Java 25+/Gradle 9 graph containing
`:aurora-native-scheduler` and `:protocol`. Root lifecycle and distribution
tasks delegate to those maintained projects.

The obsolete Java 8/Gradle 4 bootstrap and local wrapper, `buildSrc`, the old
root subprojects and plugins, and the current-source Java 8 smoke-build route
have been removed from the active build. The lab's shared Python utilities and
maintained process/container harness remain available to `clusterctl`; the
historical scheduler, Mesos adapters, Python workers, UI and auxiliary legacy
configuration remain in the source tree for the planned source-retirement step.
The removed build can be reconstructed from Git history.

The maintained root launcher pins Java/Gradle tools, defaults to Java 25, and
supports the reviewed Java 26 profiles. It uses the same cache verification and
toolchain manifest as the native packaging lane. Direct Gradle invocation is
guarded by the pinned Gradle release and Java 25 minimum.

## Verification completed

The focused checks for this implementation pass:

| Check | Result |
| --- | ---: |
| Root/maintained Java unit tests | 73 passed |
| Lab Python tests | 65 passed |
| Native packaging Python tests | 62 passed |

These checks establish the root graph and retained harness behavior. A fresh
source packaging run and fresh physical cluster qualification have **not** yet
been performed for this step. They are required before step 2 can be called
qualified; no packaging or physical result is asserted here.

## Remaining JAVA-11 work

Step 3 must inventory and retire unused legacy source, plugins and dependencies,
moving any required migration readers or fixtures into explicit maintained
boundaries. Step 4 must revisit shared SQL records and remaining Java idioms
against current consumers. The existing live MVP and stored formats are not
changed by this build-entry-point step.

See the [Java 25 baseline and convergence plan](JAVA25_BASELINE.md), the
[ordered implementation backlog](IMPLEMENTATION_BACKLOG.md), and the
[native build and qualification guide](../../build-support/native/README.md).
