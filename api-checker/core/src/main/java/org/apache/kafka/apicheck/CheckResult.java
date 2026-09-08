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

import java.util.List;

/**
 * Outcome of any of the KIP-1265 checks: the Kafka-internal cascade and javadoc validators
 * (returning would-be public-API leaks against Kafka's own surface) and the consumer-side
 * internal-usage scanner (returning references from a downstream project's bytecode to a
 * non-{@code @Public} Kafka type).
 *
 * <p>Violations are real failures that should fail the build. Suppressions are would-be
 * violations that were silenced by a class-, method-, or field-level
 * {@code @SuppressKafkaInternalApiUsage}; each carries the reason supplied to the annotation
 * so reviewers can audit every escape hatch on every build.
 */
public final class CheckResult {
    private final List<PublicApiViolation> violations;
    private final List<PublicApiViolation> suppressions;

    public CheckResult(List<PublicApiViolation> violations, List<PublicApiViolation> suppressions) {
        this.violations = violations == null ? List.of() : List.copyOf(violations);
        this.suppressions = suppressions == null ? List.of() : List.copyOf(suppressions);
    }

    public List<PublicApiViolation> violations() {
        return violations;
    }

    public List<PublicApiViolation> suppressions() {
        return suppressions;
    }

    public boolean hasViolations() {
        return !violations.isEmpty();
    }
}