# Historical commons-args project

This empty project retains Aurora's historical Gradle project identity. Neither
upstream master nor the current application has tracked source files here, and
no current project depends on it. It contributes no source, tests or runtime
artifact to application qualification.

This tracked file makes the directory present in clean checkouts. Gradle 9
requires every project declared in `settings.gradle` to have an existing project
directory; an untracked empty directory in an older workspace is insufficient.
