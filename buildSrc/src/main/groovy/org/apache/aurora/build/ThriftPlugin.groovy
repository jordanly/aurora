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
package org.apache.aurora.build

import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.FileTree
import org.gradle.process.ExecOperations
import org.gradle.api.tasks.compile.JavaCompile
import javax.inject.Inject

class ThriftPlugin implements Plugin<Project> {
  private final ExecOperations execOperations

  @Inject
  ThriftPlugin(ExecOperations execOperations) {
    this.execOperations = execOperations
  }

  @Override
  void apply(Project project) {
    project.configure(project) {
      apply plugin: 'java'

      extensions.create('thrift', ThriftPluginExtension, project)

      configurations.create('thriftCompile')
      afterEvaluate {
        dependencies {
          thriftCompile "org.apache.thrift:libthrift:${thrift.version}"
          thriftCompile 'javax.annotation:javax.annotation-api:1.3.2'
        }
      }

      task('checkThriftCompiler') {
        doLast {
          if (thrift.compilerPath == null) {
            throw new GradleException(
                'thriftCompiler must name an explicit Thrift compiler; ' +
                'the legacy thriftw/Pants fallback is unsupported.')
          }
          if (!thrift.compilerPath.isFile()) {
            throw new GradleException("Thrift compiler does not exist: ${thrift.compilerPath}")
          }
          def versionOutput = new ByteArrayOutputStream()
          execOperations.exec {
            commandLine thrift.compilerPath, '--version'
            standardOutput = versionOutput
          }
          if (versionOutput.toString('UTF-8').trim() != "Thrift version ${thrift.version}") {
            throw new GradleException("Expected Thrift ${thrift.version}: ${versionOutput}")
          }
        }
      }

      task('generateThriftJava', dependsOn: 'checkThriftCompiler') {
        inputs.files {thrift.inputFiles}
        inputs.files {thrift.compilerPath}
        inputs.property('thriftVersion') {thrift.version}
        outputs.dir {thrift.genJavaDir}
        doLast {
          delete thrift.genJavaDir
          thrift.genJavaDir.mkdirs()
          thrift.inputFiles.sort().each { File file ->
            execOperations.exec {
              commandLine thrift.compilerCommand() + [
                  // Keep generated source reproducible; this changes annotations only,
                  // leaving the Thrift wire/API contract unchanged.
                  '--gen', 'java:private-members,generated_annotations=undated',
                  '-out', thrift.genJavaDir.path, file.path]
            }
          }
        }
      }

      task('generateThriftResources', dependsOn: 'checkThriftCompiler') {
        inputs.files {thrift.inputFiles}
        inputs.files {thrift.compilerPath}
        inputs.property('thriftVersion') {thrift.version}
        outputs.dir {thrift.genResourcesDir}
        doLast {
          delete thrift.genResourcesDir
          def dest = file("${thrift.genResourcesDir}/${thrift.resourcePrefix}")
          dest.exists() || dest.mkdirs()
          thrift.inputFiles.sort().each { File file ->
            execOperations.exec {
              commandLine thrift.compilerCommand() + [
                  '--gen', 'js:jquery',
                  '--gen', 'html:standalone',
                  '-out', dest.path, file.path]
            }
          }
        }
      }

      task('classesThrift', type: JavaCompile) {
        source files(generateThriftJava)
        classpath = configurations.thriftCompile
        destinationDirectory = file(thrift.genClassesDir)
        options.warnings = false
        // Capture method parameter names in classfiles.
        options.compilerArgs << '-parameters'
      }

      configurations.create('thriftRuntime')
      configurations.thriftRuntime.extendsFrom(configurations.thriftCompile)
      // Export library dependencies only. Generated classes are project output and
      // travel in the API jar, not as directory dependencies in distributions.
      configurations.api.extendsFrom(configurations.thriftCompile)
      dependencies {
        thriftRuntime files(thrift.genClassesDir).builtBy(classesThrift)
      }

      sourceSets.main {
        output.classesDirs.from(files(thrift.genClassesDir).builtBy(classesThrift))
        output.dir(generateThriftResources)
      }
    }
  }
}

class ThriftPluginExtension {
  def wrapperPath
  File compilerPath
  File genResourcesDir
  File genJavaDir
  File genClassesDir
  FileTree inputFiles

  List compilerCommand() {
    [compilerPath.path]
  }

  String version
  String getVersion() {
    if (version == null) {
      throw new GradleException('thrift.version is required.')
    } else {
      return version
    }
  }

  String resourcePrefix

  /* Classpath prefix for generated resources. */
  String getResourcesPrefix() {
    if (resourcePrefix == null) {
      throw new GradleException('thrift.resourcePrefix is required.')
    } else {
      return resourcePrefix
    }
  }

  ThriftPluginExtension(Project project) {
    if (project.hasProperty('thriftCompiler')) {
      compilerPath = project.file(project.property('thriftCompiler'))
    }
    wrapperPath = "${project.rootDir}/build-support/thrift/thriftw"
    genResourcesDir = project.file("${project.buildDir}/thrift/gen-resources")
    genJavaDir = project.file("${project.buildDir}/thrift/gen-java")
    genClassesDir = project.file("${project.buildDir}/thrift/classes")
    inputFiles = project.fileTree("src/main/thrift").matching {
      include "**/*.thrift"
    }
  }
}
