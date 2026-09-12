namespace java org.apache.aurora.gen
enum Shade { RED = 1, BLUE = 2 }
struct Leaf { 1: string label }
struct AllTypes {
  1: optional bool enabled
  2: i32 small
  3: required i64 large
  4: double ratio
  5: binary payload
  6: string HTTPName
  7: Shade shade
  8: Leaf leaf
  9: list<Leaf> leaves
  10: set<Leaf> uniqueLeaves
  11: list<Shade> shades
  12: map<Shade, string> names
  13: map<i32, i64> counts
  14: LIST<string> strings
}
union Choice {
  1: bool enabled
  2: i32 small
  3: i64 large
  4: double ratio
  5: binary payload
  6: string text
  7: Leaf child
}
service Root { void ping() }
service Middle extends Root { void middle(1: binary data, 2: double ratio) }
service AuroraAdmin extends Middle { void put(1: AllTypes value, 2: map<string, i64> counts, 3: set<Shade> shades) }
