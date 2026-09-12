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
package org.apache.aurora.scheduler.storage.sqlite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;

/** Rejects unsupported fields instead of letting Thrift silently discard them during import. */
final class StrictSnapshot {
  private StrictSnapshot() { }

  static void validate(byte[] bytes) {
    try {
      TMemoryInputTransport input = new TMemoryInputTransport(bytes);
      struct(new TBinaryProtocol(input), Snapshot.class, 0);
      if (input.getBytesRemainingInBuffer() != 0) {
        throw new StorageException("Trailing bytes after historical snapshot");
      }
    } catch (TException | ReflectiveOperationException e) {
      throw new StorageException("Malformed or unsupported historical snapshot", e);
    }
  }

  @SuppressWarnings("rawtypes")
  private static void struct(TProtocol protocol, Class<? extends TBase> type, int depth)
      throws TException, ReflectiveOperationException {
    require(depth <= 64, "Historical snapshot nesting limit");
    Map<Short, FieldMetaData> fields = new HashMap<>();
    for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> field
        : FieldMetaData.getStructMetaDataMap(type).entrySet()) {
      fields.put(field.getKey().getThriftFieldId(), field.getValue());
    }
    Set<Short> seen = new HashSet<>();
    protocol.readStructBegin();
    while (true) {
      var field = protocol.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      FieldMetaData metadata = fields.get(field.id);
      if (metadata == null || !seen.add(field.id)) {
        throw new StorageException(
            "Unknown or repeated historical field " + type.getSimpleName() + "." + field.id);
      }
      require(field.type == wireType(metadata.valueMetaData), "Historical field type mismatch");
      value(protocol, metadata.valueMetaData, depth + 1);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  private static void value(TProtocol protocol, FieldValueMetaData metadata, int depth)
      throws TException, ReflectiveOperationException {
    require(depth <= 64, "Historical snapshot nesting limit");
    if (metadata instanceof StructMetaData nested) {
      struct(protocol, nested.structClass, depth + 1);
    } else if (metadata instanceof EnumMetaData enumeration) {
      int number = protocol.readI32();
      require(enumeration.enumClass.getMethod("findByValue", int.class)
          .invoke(null, number) != null, "Unknown historical enum value");
    } else if (metadata instanceof SetMetaData set) {
      var container = protocol.readSetBegin();
      count(container.size);
      require(container.elemType == wireType(set.elemMetaData), "Historical set type mismatch");
      for (int i = 0; i < container.size; i++) {
        value(protocol, set.elemMetaData, depth + 1);
      }
      protocol.readSetEnd();
    } else if (metadata instanceof ListMetaData list) {
      var container = protocol.readListBegin();
      count(container.size);
      require(container.elemType == wireType(list.elemMetaData), "Historical list type mismatch");
      for (int i = 0; i < container.size; i++) {
        value(protocol, list.elemMetaData, depth + 1);
      }
      protocol.readListEnd();
    } else if (metadata instanceof MapMetaData map) {
      var container = protocol.readMapBegin();
      count(container.size);
      require(container.keyType == wireType(map.keyMetaData)
          && container.valueType == wireType(map.valueMetaData), "Historical map type mismatch");
      for (int i = 0; i < container.size; i++) {
        value(protocol, map.keyMetaData, depth + 1);
        value(protocol, map.valueMetaData, depth + 1);
      }
      protocol.readMapEnd();
    } else {
      TProtocolUtil.skip(protocol, metadata.type);
    }
  }

  private static byte wireType(FieldValueMetaData metadata) {
    return metadata instanceof EnumMetaData ? TType.I32 : metadata.type;
  }

  private static void count(int count) {
    require(count >= 0 && count <= 1_000_000, "Historical snapshot collection limit");
  }

  private static void require(boolean accepted, String message) {
    if (!accepted) {
      throw new StorageException(message);
    }
  }
}
