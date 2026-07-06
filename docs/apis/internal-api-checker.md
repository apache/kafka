---
title: Internal API Checker
description: Detect references to non-public Kafka classes in your project's compiled bytecode.
weight: 7
tags: ['kafka', 'docs']
aliases:
keywords:
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Internal API Checker

[KIP-1265](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1265%3A+Mechanism+for+automatic+detection+of+internal+API+usage)
ships a build-time checker that flags references from your compiled bytecode to Kafka classes
that are **not** marked `@InterfaceAudience.Public`. Apply it to your connector, Streams
application, or any project that depends on `org.apache.kafka:*` to catch internal-API usage
that would otherwise silently break when you upgrade Kafka.

Bytecode scanning works uniformly for Java, Scala, Kotlin, and any other JVM language —
unlike a source-level grep on `import` lines, the checker catches fully-qualified references,
wildcard-imported types, and references introduced by code generators or compiler intrinsics.

## Gradle

```groovy
plugins {
    id 'org.apache.kafka.internal-api-checker' version '{{< param fullDotVersion >}}'
}
```

The plugin registers a `kafkaInternalApiChecker` task in the `verification` group and hooks
it onto `check`, so `./gradlew check` will fail the build on any unsuppressed reference to an
internal Kafka class.

Defaults are derived from the `java` plugin: the scan targets `sourceSets.main.output.classesDirs`
(works for Java, Scala, and Kotlin projects uniformly), and the `@InterfaceAudience.Public`
surface is built from the `org.apache.kafka:*` artifacts on `compileClasspath` and
`testCompileClasspath`. A Kafka version bump invalidates the task, so the check re-runs against
the new surface.

Override only when the defaults don't fit:

```groovy
kafkaInternalApiChecker {
    enabled         = true                  // default
    failOnViolation = true                  // default
    classDirs.from(files('extra-classes'))  // extend the bytecode roots
    // Replace if you keep Kafka jars on a non-standard configuration:
    // kafkaDependencyJars.setFrom(configurations.myKafkaBundle)
}
```

### Skipping the check for one invocation

Pass `-PkafkaInternalApiChecker.skip` (any truthy value, or no value at all) to disable the
check for that Gradle invocation without editing the build script:

```bash
./gradlew check -PkafkaInternalApiChecker.skip
```

## Maven

```xml
<plugin>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-internal-api-checker-maven-plugin</artifactId>
  <version>{{< param fullDotVersion >}}</version>
  <executions>
    <execution>
      <phase>verify</phase>
      <goals><goal>verify</goal></goals>
    </execution>
  </executions>
</plugin>
```

The mojo binds to the `verify` phase and reads `${project.build.outputDirectory}` by default.

## Reports

Each run writes a text report to `build/reports/kafka-internal-api-usage.txt`
(`target/` for Maven). The report groups violations by type and by class, and lists any
suppressions separately so they can be audited.

## Suppressing known references

When a reference to an internal class is intentional — typically because the public-API
alternative is still being designed — annotate the class, method, or field with
`@SuppressKafkaInternalApiUsage` and include a one-line reason:

```java
import org.apache.kafka.common.annotation.SuppressKafkaInternalApiUsage;

public class MyConnector implements SinkConnector {

    @SuppressKafkaInternalApiUsage("KIP-XYZ: replace with public API once finalised")
    private final InternalKafkaHelper helper = new InternalKafkaHelper();
}
```

Suppressed references move from the violations section of the report into a dedicated
**Suppressions** section, together with the reason supplied to the annotation, so
reviewers can audit every escape hatch on every build.

The annotation lives in `kafka-clients`:

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>{{< param fullDotVersion >}}</version>
</dependency>
```

## Kafka version requirement

The checker reads `@InterfaceAudience.Public` annotations off the **Kafka libraries your
project depends on at compile time**, not off the version of Kafka the checker plugin
itself was published from. If your project still depends on a Kafka release that pre-dates
KIP-1265 — i.e., one that doesn't yet carry the audience annotations on any class — the
checker will see zero public APIs and report every `org.apache.kafka.*` reference in your
bytecode as a violation, including references to genuinely-public types like
`KafkaProducer` or `Topology`.

Make sure each `org.apache.kafka:*` dependency on your compile classpath
(`kafka-clients`, `kafka-streams`, `connect-api`, `kafka-tools-api`, …) is at least
`{{< param fullDotVersion >}}` before turning `failOnViolation = true`. For older
dependencies, either upgrade them, or temporarily set `failOnViolation = false` so the
checker only generates reports while you migrate.

## What counts as "internal"

A Kafka class is considered public when:

1. It carries `@InterfaceAudience.Public` directly, **or**
2. It is a nested class whose nearest annotated enclosing class is `@InterfaceAudience.Public`
   (Hadoop-style audience inheritance — see the KIP for details).

Classes outside `org.apache.kafka.*` are out of scope. `@Deprecated` is handled
asymmetrically:

- **Producer side** (Kafka's own `docsJar` cascade): a `@Deprecated` reference is treated
  as out of scope so an already-deprecated public method that still names an internal type
  doesn't show up as a fresh leak.
- **Consumer side** (this checker): a `@Deprecated` *internal* class is still flagged, on
  the grounds that the deprecated ones are the most likely to be removed in the next
  release. Consumers should migrate off them, not lean on them.

## Known limitations

The bytecode scanner walks every callable instruction, field type, declared exception, and
generic signature in your compiled classes, but a few reference kinds are intentionally not
followed. In practice these don't show up in plugin/connector code, but a "0 violations"
report does not strictly exclude them:

- **Parameter annotations.** A `@InternalAnno`-typed parameter annotation isn't walked
  (the method header itself still is, so the parameter's type and the method's return /
  exception types are all caught).
- **Type-use annotations.** JSR 308 type-use annotations attached to type positions like
  `List<@InternalAnno String>` aren't walked — these are typically used only by
  static-analysis tooling, and the underlying type is still recorded.
- **Class literals exclusively inside annotation element values.** A class literal that
  *only* appears in an annotation value (e.g. `@SomeAnnotation(impl = InternalClass.class)`)
  won't be flagged. The same `InternalClass.class` loaded into any local variable or method
  return *is* caught via `LDC` instructions.
- **Inlined compile-time constants.** Java inlines `public static final` primitive and
  `String` constants at the use site, so referencing
  `InternalKafkaClass.SOME_CONSTANT_STRING` leaves no class reference in your bytecode and
  cannot be detected. This is documented in the KIP.
