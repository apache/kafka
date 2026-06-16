# Migration Notes

Curated false positives remaining after the data-driven migration of `build.gradle`.
All Category C operator-mutation hits (`compilerArgs <<`/`+=`, `additionalParameters +=`)
were real and have been rewritten to `.add()` / `.addAll()`. The entries below are scanner
hits whose receiver is **not** the migrated Gradle type the scanner guessed, so they are
left unchanged.

### `build.gradle` — false positives

- line 113 [Cat-B]: `project.getRootDir()` is `org.gradle.api.Project.getRootDir()` returning `File`, not the scanner's guessed `org.gradle.vcs.VersionControlSpec.rootDir` — Project paths are not lazy-migrated.
  - source: `  repo = file("$rootDir/.git").isDirectory() ? Grgit.open(currentDir: project.getRootDir()) : null`
- line 4236 [Cat-B]: `it.javadoc.getIncludes()` reads the `org.gradle.api.tasks.javadoc.Javadoc` task's `PatternFilterable` includes (`Set<String>`), not `JacocoTaskExtension.includes` — `PatternFilterable.includes` is absent from migration-data (the Jacoco entry has empty `inheriting_subtypes`).
  - source: `    includes  = projectsWithJavadoc.collectMany { it.javadoc.getIncludes() }`
- line 4237 [Cat-B]: `it.javadoc.getExcludes()` reads the `Javadoc` task's `PatternFilterable` excludes (`Set<String>`), not `JacocoTaskExtension.excludes` — a `PatternFilterable` filter, not a migrated lazy property.
  - source: `    excludes  = projectsWithJavadoc.collectMany { it.javadoc.getExcludes() }`
