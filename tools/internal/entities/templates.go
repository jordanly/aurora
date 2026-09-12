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

// Templates preserve the historical wrapper source bytes and public API.
const fieldTemplate = `  public %(type)s %(fn_name)s() {
    return %(field)s;
  }`

const unionFieldTemplate = `  public %(type)s %(fn_name)s() {
    if (getSetField() == %(enum_value)s) {
      return (%(type)s) value;
    } else {
      throw new RuntimeException("Cannot get field '%(enum_value)s' "
          + "because union is currently set to " + getSetField());
    }
  }`

const unionSwitchCase = `case %(case)s:
        %(body)s`

const unionDefaultError = `throw new RuntimeException("Unrecognized field " + getSetField())`

const unionFieldSwitch = `switch (%(switch_by)s) {
      %(cases)s
      default:
        %(error)s;
    }`

const unionCopyConstructor2 = `  public static %(wrapped)s newBuilder(int id, Object value) {
    %(body)s
  }`

const unionValueAccessor = `  public Object getRawValue() {
    return value;
  }`

const simpleAssignment = `this.%(field)s = wrapped.%(fn_name)s();`

const fieldDeclaration = `private final %(type)s %(field)s;`

const structAssignment = `this.%(field)s = wrapped.%(isset)s()
        ? %(type)s.build(wrapped.%(fn_name)s())
        : null;`

const immutableCollectionAssignment = `this.%(field)s = wrapped.%(isset)s()
        ? Immutable%(collection)s.copyOf(wrapped.%(fn_name)s())
        : Immutable%(collection)s.of();`

const structCollectionFieldAssignment = `this.%(field)s = wrapped.%(isset)s()
        ? FluentIterable.from(wrapped.%(fn_name)s())
              .transform(%(params)s::build)
              .to%(collection)s()
        : Immutable%(collection)s.<%(params)s>of();`

const packageName = `org.apache.aurora.scheduler.storage.entities`

const classTemplate = `package %(package)s;

%(imports)s

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 */
public final class %(name)s {
  private int cachedHashCode = 0;
%(fields)s
  private %(name)s(%(wrapped)s wrapped) {%(assignments)s
  }

  public static %(name)s build(%(wrapped)s wrapped) {
    return new %(name)s(wrapped);
  }

  public static ImmutableList<%(wrapped)s> toBuildersList(Iterable<%(name)s> w) {
    return FluentIterable.from(w).transform(%(name)s::newBuilder).toList();
  }

  static List<%(wrapped)s> toMutableBuildersList(Iterable<%(name)s> w) {
    return Lists.newArrayList(Iterables.transform(w, %(name)s::newBuilder));
  }

  public static ImmutableList<%(name)s> listFromBuilders(Iterable<%(wrapped)s> b) {
    return FluentIterable.from(b).transform(%(name)s::build).toList();
  }

  public static ImmutableSet<%(wrapped)s> toBuildersSet(Iterable<%(name)s> w) {
    return FluentIterable.from(w).transform(%(name)s::newBuilder).toSet();
  }

  static Set<%(wrapped)s> toMutableBuildersSet(Iterable<%(name)s> w) {
    return Sets.newHashSet(Iterables.transform(w, %(name)s::newBuilder));
  }

  public static ImmutableSet<%(name)s> setFromBuilders(Iterable<%(wrapped)s> b) {
    return FluentIterable.from(b).transform(%(name)s::build).toSet();
  }

  public %(wrapped)s newBuilder() {
    %(copy_constructor)s
  }

%(accessors)s

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof %(name)s)) {
      return false;
    }
    %(name)s other = (%(name)s) o;
    return %(equals)s;
  }

  @Override
  public int hashCode() {
    // Following java.lang.String's example of caching hashCode.
    // This is thread safe in that multiple threads may wind up
    // computing the value, which is apparently favorable to constant
    // synchronization overhead.
    if (cachedHashCode == 0) {
      cachedHashCode = Objects.hash(%(hashcode)s);
    }
    return cachedHashCode;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)%(to_string)s
        .toString();
  }
}`

const paramMetadataTemplate = `%(type)s.class,`

const methodMetadataTemplate = `.put(
              "%(name)s",
              new Class<?>[] {%(params)s
                  })`

const serviceMetadataTemplate = `package %(package)s;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.*;

public final class %(name)sMetadata {
  public static final ImmutableMap<String, Class<?>[]> METHODS =
      ImmutableMap.<String, Class<?>[]>builder()
          %(methods)s
          .build();

  private %(name)sMetadata() {
    // Utility class
  }
}
`
