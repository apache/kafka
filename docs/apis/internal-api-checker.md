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

Apply the plugin and point it at your project's compiled classes (any of: a `classes/`
directory, a single `.class` file, or a `.jar`):

```groovy
plugins {
    id 'org.apache.kafka.internal-api-checker' version '{{< param fullDotVersion >}}'
}

kafkaInternalApiChecker {
    enabled         = true
    failOnViolation = true
    classDirs       = files('build/classes')   // default
}
```

The plugin registers a `kafkaInternalApiChecker` task in the `verification` group.
It runs as part of `check` by default, so `./gradlew check` will fail the build on any
unsuppressed reference to an internal Kafka class.

## Maven

```xml
<plugin>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-internal-api-checker-maven-plugin</artifactId>
  <version>{{< param fullDotVersion >}}</version>
  <executions>
    <execution>
      <phase>verify</phase>
      <goals><goal>check</goal></goals>
    </execution>
  </executions>
</plugin>
```

The mojo binds to the `verify` phase and reads `${project.build.outputDirectory}` by default.

## Reports

Each run writes both text and JSON reports under `build/reports/kafka-internal-api-usage.{txt,json}`
(`target/` for Maven). The reports group violations by type and by class, and list any
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

Classes outside `org.apache.kafka.*` are out of scope; classes carrying `@Deprecated` are
treated as out of scope on both sides of the check so deprecated public APIs you still
reference don't appear as violations.
