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

import java.util.List;

/**
 * Outcome of a public-API check. Violations are real failures that should fail the build;
 * suppressions are would-be cascade violations that were silenced because the containing
 * method or class carries {@code @SuppressKafkaInternalApiUsage}. Every suppression carries
 * the reason from the annotation so reviewers can audit every escape hatch on every build.
 */
public final class CheckResult {
    private final List<PublicApiViolation> violations;
    private final List<PublicApiViolation> suppressions;

    public CheckResult(List<PublicApiViolation> violations, List<PublicApiViolation> suppressions) {
        this.violations = violations;
        this.suppressions = suppressions;
    }

    public List<PublicApiViolation> violations() {
        return violations;
    }

    public List<PublicApiViolation> suppressions() {
        return suppressions;
    }
}