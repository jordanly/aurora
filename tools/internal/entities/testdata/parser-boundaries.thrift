namespace py ignored.namespace
namespace java ignored.first
namespace java org.apache.aurora.gen
include "not-loaded.thrift"
struct Empty { }
struct Qualified {
  1: external.Child ignored
  2: list<list<string>> nestedIgnored
  3: string retained = "default"
}
exception NotWrapped { 1: string reason }
service AuroraAdmin { void call(1: bool enabled, 2: optional i32 skipped) }
