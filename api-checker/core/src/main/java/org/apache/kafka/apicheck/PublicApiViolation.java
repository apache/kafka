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

/**
 * Represents a violation of the public API rules.
 */
public class PublicApiViolation {
    /**
     * Rendered into a suppression's description when the {@code @SuppressKafkaInternalApiUsage}
     * annotation carried no {@code value()}. Stable marker so the reporter and the build tasks
     * can surface unjustified suppressions consistently.
     */
    public static final String NO_REASON_MARKER = "(no reason given)";

    private final String className;
    private final String violationType;
    private final String description;
    private final String memberName;
    private final boolean lacksReason;

    public PublicApiViolation(String className, String violationType, String description, String memberName) {
        this(className, violationType, description, memberName, false);
    }

    /**
     * Suppression-constructor variant: pass {@code lacksReason=true} when the underlying
     * {@code @SuppressKafkaInternalApiUsage} carried no {@code value()}. KIP-1265 makes the
     * reason required, so the reporter surfaces unjustified suppressions as a warning.
     */
    public PublicApiViolation(String className, String violationType, String description,
                              String memberName, boolean lacksReason) {
        this.className = className;
        this.violationType = violationType;
        this.description = description;
        this.memberName = memberName;
        this.lacksReason = lacksReason;
    }

    public String getClassName() {
        return className;
    }

    public String getViolationType() {
        return violationType;
    }

    public String getDescription() {
        return description;
    }

    public String getMemberName() {
        return memberName;
    }

    @Override
    public String toString() {
        if (memberName != null && !memberName.isEmpty()) {
            return String.format("[%s] %s.%s: %s", violationType, className, memberName, description);
        } else {
            return String.format("[%s] %s: %s", violationType, className, description);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PublicApiViolation that = (PublicApiViolation) o;

        if (!className.equals(that.className)) return false;
        if (!violationType.equals(that.violationType)) return false;
        if (!description.equals(that.description)) return false;
        return memberName != null ? memberName.equals(that.memberName) : that.memberName == null;
    }

    /**
     * @return true iff this represents a suppression whose {@code @SuppressKafkaInternalApiUsage}
     *         annotation carried no {@code value()}. KIP-1265 makes the reason required, so the
     *         checker warns on unjustified suppressions.
     */
    public boolean lacksReason() {
        return lacksReason;
    }

    @Override
    public int hashCode() {
        int result = className.hashCode();
        result = 31 * result + violationType.hashCode();
        result = 31 * result + description.hashCode();
        result = 31 * result + (memberName != null ? memberName.hashCode() : 0);
        return result;
    }
}