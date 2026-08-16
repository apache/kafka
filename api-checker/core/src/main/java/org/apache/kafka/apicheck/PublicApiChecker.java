/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.apicheck;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Facade for the KIP-1265 public-API checker. Pre-scans the supplied Kafka jars into an
 * {@link ApiSurface} once and delegates each check to a focused validator:
 * <ul>
 *   <li>{@link JavadocConsistencyValidator} for {@code MISSING_JAVADOC} / {@code MISSING_PUBLICAPI_ANNOTATION}.</li>
 *   <li>{@link CascadeValidator} for {@code INVALID_RETURN_TYPE} / {@code INVALID_PARAMETER_TYPE} /
 *       {@code INVALID_EXCEPTION_TYPE} with {@code @SuppressKafkaInternalApiUsage} suppression.</li>
 *   <li>{@link PluginDeveloperApiUsageScanner} for the consumer-side check (via {@link #checkBytecode}).</li>
 * </ul>
 *
 * <p>All bytecode reading is done via ASM; no classloading, no {@code LinkageError} risk from
 * broken transitive deps.
 */
public class PublicApiChecker {

    private final ApiSurface surface;

    /**
     * @param projectJars   jars produced by the project being checked. Their classes drive
     *                      the MISSING_JAVADOC iteration and are cascade-checked for method
     *                      signature leaks.
     * @param referenceJars jars from sibling Kafka modules this project depends on. Their
     *                      classes contribute to the {@code @InterfaceAudience.Public}
     *                      membership set so cross-module references resolve, but they don't
     *                      take part in this project's own javadoc consistency or cascade
     *                      iteration (each module checks its own surface).
     */
    public PublicApiChecker(List<File> projectJars, List<File> referenceJars) throws IOException {
        this.surface = ApiSurfaceScanner.scan(projectJars, referenceJars);
    }

    /** Convenience for the consumer-side scanner: no separate reference jars needed. */
    public PublicApiChecker(List<File> kafkaJars) throws IOException {
        this(kafkaJars, java.util.Collections.emptyList());
    }

    /**
     * Cross-validate the javadoc HTML against the project's bytecode audience annotations and
     * cascade-check public method signatures for internal-type leaks.
     */
    public CheckResult checkPublicApiConsistency(File javadocJar) throws IOException {
        CheckResult javadoc = JavadocConsistencyValidator.validate(javadocJar, surface);
        CheckResult cascade = CascadeValidator.validate(surface);

        List<PublicApiViolation> violations = new ArrayList<>(javadoc.violations());
        violations.addAll(cascade.violations());
        List<PublicApiViolation> suppressions = new ArrayList<>(javadoc.suppressions());
        suppressions.addAll(cascade.suppressions());
        return new CheckResult(violations, suppressions);
    }

    /**
     * Consumer-side check: walk compiled bytecode at the given roots and flag references to any
     * {@code org.apache.kafka.*} class that isn't effectively {@code @InterfaceAudience.Public}.
     * Roots may be class directories or jar archives.
     */
    public CheckResult checkBytecode(List<File> classFileRoots) throws IOException {
        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(this::isPublicApi);
        return scanner.scan(classFileRoots);
    }

    /**
     * Filter the supplied class-file roots to those that exist on disk. Both adapters
     * (Gradle task, Maven mojo) seed the input from build configuration that may include
     * directories the build hasn't produced yet (e.g. test classes when there are no tests).
     */
    public static List<File> collectExistingRoots(Iterable<File> roots) {
        List<File> existing = new ArrayList<>();
        for (File root : roots) {
            if (root.exists()) {
                existing.add(root);
            }
        }
        return existing;
    }

    /**
     * Consumer-side predicate: true if the binary class name (e.g.
     * {@code org.apache.kafka.clients.producer.KafkaProducer} or
     * {@code org.apache.kafka.clients.admin.OffsetSpec$LatestSpec}) is effectively
     * {@code @InterfaceAudience.Public} — either via a direct annotation or by inheritance
     * from an enclosing class. A direct {@code @InterfaceAudience.Private} overrides an
     * inherited Public. Non-{@code org.apache.kafka.*} types are treated as out of scope.
     *
     * <p>Deprecation is intentionally <em>not</em> a bypass on the consumer side:
     * {@code @Deprecated} internal types are exactly the ones most likely to be removed in
     * the next release, so consumer references to them deserve a violation. The cascade
     * validator has its own deprecation bypass for the producer-side leak check, which is
     * fine — that side is just asking "did we expose this in the API surface?", whereas the
     * consumer side is asking "am I going to break when Kafka removes this?".
     */
    public boolean isPublicApi(String binaryClassName) {
        if (!binaryClassName.startsWith("org.apache.kafka.")) {
            return true; // not in scope
        }
        return surface.isEffectivelyPublic(binaryClassName);
    }
}