/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.nativeprotocol;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Local protocol conformance CLI; canonical bytes on success, no payload in errors. */
public final class ProtocolTool {
  private ProtocolTool() { }
  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        throw new IllegalArgumentException("usage: protocol document.json");
      }
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (InputStream stream = Files.newInputStream(Paths.get(args[0]))) {
        byte[] block = new byte[4096];
        int count;
        while ((count = stream.read(block)) != -1) {
          if (bytes.size() + count > ProtocolValidator.MAX_BYTES) {
            throw new IllegalArgumentException("document exceeds size limit");
          }
          bytes.write(block, 0, count);
        }
      }
      ProtocolValidator.Message message = new ProtocolValidator().validate(bytes.toByteArray());
      System.out.write(message.canonicalBytes());
      System.out.write('\n');
    } catch (Exception error) {
      System.err.println("native protocol validation failed (" + error.getClass().getSimpleName() + ")");
      System.exit(2);
    }
  }
}
