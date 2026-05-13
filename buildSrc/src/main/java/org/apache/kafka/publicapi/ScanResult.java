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
package org.apache.kafka.publicapi;

import java.util.Collections;
import java.util.List;

/**
 * Result of a bytecode scan: the set of public-API violations that should fail the build,
 * plus the set of references that were skipped because of a {@code @SuppressKafkaInternalApiUsage}
 * annotation on the consumer. Suppressions are surfaced in the report (with each annotation's
 * reason) so reviewers can audit them.
 */
public final class ScanResult {
    private final List<PublicApiViolation> violations;
    private final List<PublicApiViolation> suppressions;

    public ScanResult(List<PublicApiViolation> violations, List<PublicApiViolation> suppressions) {
        this.violations = violations == null ? Collections.emptyList() : violations;
        this.suppressions = suppressions == null ? Collections.emptyList() : suppressions;
    }

    public List<PublicApiViolation> getViolations() {
        return violations;
    }

    public List<PublicApiViolation> getSuppressions() {
        return suppressions;
    }

    public boolean hasViolations() {
        return !violations.isEmpty();
    }
}
