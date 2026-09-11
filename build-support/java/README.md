# Historical Java build support

The Java 8, Gradle 4.10.2 and Thrift 0.10.0 ARM64 bootstrap lane has been retired.
Its bootstrap script, local Gradle launcher and archive checksum manifest were
historical reconstruction machinery, not a supported development entry point.

The remaining files are comparison fixtures for the historical Thrift wrapper
generator:

- `test_thrift_wrapper_codegen.py` keeps the focused regression cases.
- `wrapper-api-sha256.json` keeps the source-derived generated API golden.

These fixtures document prior behavior and do not define a supported Java runtime,
bytecode level or build path. See the maintained native build guidance in
[build-support/native/README.md](../native/README.md) and the repository Java 25+
baseline in [docs/reimagining/JAVA25_BASELINE.md](../../docs/reimagining/JAVA25_BASELINE.md).
