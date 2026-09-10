#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ $(uname -s) != Linux || $(uname -m) != aarch64 ]]; then
  echo 'This temporary toolchain is pinned for Linux aarch64 only.' >&2
  exit 1
fi
tool_dir="$repo/.pi-tools"
mkdir -p "$tool_dir/downloads"
cd "$tool_dir/downloads"
fetch() {
  local name=$1 url=$2
  if [[ ! -f $name ]]; then
    curl --fail --location --max-time 180 "$url" --output "$name.part"
    mv "$name.part" "$name"
  fi
}
fetch OpenJDK8U-jdk_aarch64_linux_hotspot_8u462b08.tar.gz \
  https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u462-b08/OpenJDK8U-jdk_aarch64_linux_hotspot_8u462b08.tar.gz
fetch gradle-4.10.2-bin.zip https://services.gradle.org/distributions/gradle-4.10.2-bin.zip
fetch thrift-0.10.0.tar.gz https://archive.apache.org/dist/thrift/0.10.0/thrift-0.10.0.tar.gz
sha256sum --check "$repo/build-support/java/toolchains.sha256"
if [[ ! -d $tool_dir/jdk8u462-b08 ]]; then
  tar -xzf OpenJDK8U-jdk_aarch64_linux_hotspot_8u462b08.tar.gz -C "$tool_dir"
fi
if [[ ! -d $tool_dir/gradle-4.10.2 ]]; then
  unzip -q gradle-4.10.2-bin.zip -d "$tool_dir"
fi
if [[ ! -d $tool_dir/thrift-0.10.0 ]]; then
  tar -xzf thrift-0.10.0.tar.gz -C "$tool_dir"
fi
if [[ ! -x $tool_dir/thrift-0.10.0/compiler/cpp/thrift ]]; then
  cd "$tool_dir/thrift-0.10.0"
  ./configure --without-libs --without-tests --without-tutorial
  make -C compiler/cpp -j2
fi
"$tool_dir/jdk8u462-b08/bin/java" -version
"$tool_dir/thrift-0.10.0/compiler/cpp/thrift" --version
