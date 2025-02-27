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
package org.apache.kafka.streams.processor.internals;

public final class InternalResourcesNaming {

    private final String repartitionTopic;
    private final String changelogTopic;
    private final String stateStore;

    private InternalResourcesNaming(final Builder builder) {
        this.repartitionTopic = builder.repartitionTopic;
        this.changelogTopic = builder.changelogTopic;
        this.stateStore = builder.stateStore;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String repartitionTopic;
        private String changelogTopic;
        private String stateStore;

        private Builder() {}

        public Builder withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        public Builder withChangelogTopic(final String changelogTopic) {
            this.changelogTopic = changelogTopic;
            return this;
        }

        public Builder withStateStore(final String stateStore) {
            this.stateStore = stateStore;
            return this;
        }

        public InternalResourcesNaming build() {
            return new InternalResourcesNaming(this);
        }
    }

    public String repartitionTopic() {
        return repartitionTopic;
    }

    public String changelogTopic() {
        return changelogTopic;
    }

    public String stateStore() {
        return stateStore;
    }
}