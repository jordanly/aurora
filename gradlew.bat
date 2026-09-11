@echo off
rem Licensed under the Apache License, Version 2.0 (the "License");
rem you may not use this file except in compliance with the License.
rem You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
rem Unless required by applicable law or agreed to in writing, software distributed
rem under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
rem CONDITIONS OF ANY KIND, either express or implied. See the License for the
rem specific language governing permissions and limitations under the License.
>&2 echo The pinned root build requires Linux ARM64. Run ./gradlew on that platform.
exit /b 1
