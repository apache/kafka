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

package org.apache.kafka.connect.openlineage;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Holds the input and output datasets, plus the job type, extracted from a
 * connector's configuration by a {@link ConnectorVisitor}.
 */
public final class ConnectorLineage {

    /**
     * A single OpenLineage dataset reference, consisting of a namespace and
     * a name.
     */
    public static final class Dataset {
        private final String namespace;
        private final String name;

        public Dataset(String namespace, String name) {
            this.namespace = Objects.requireNonNull(namespace, "namespace");
            this.name = Objects.requireNonNull(name, "name");
        }

        public String namespace() {
            return namespace;
        }

        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Dataset dataset = (Dataset) o;
            return namespace.equals(dataset.namespace) && name.equals(dataset.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, name);
        }

        @Override
        public String toString() {
            return "Dataset{namespace='" + namespace + "', name='" + name + "'}";
        }
    }

    private final List<Dataset> inputs;
    private final List<Dataset> outputs;
    private final String jobType;

    public ConnectorLineage(List<Dataset> inputs, List<Dataset> outputs, String jobType) {
        this.inputs = inputs != null ? Collections.unmodifiableList(inputs) : Collections.emptyList();
        this.outputs = outputs != null ? Collections.unmodifiableList(outputs) : Collections.emptyList();
        this.jobType = Objects.requireNonNull(jobType, "jobType");
    }

    public List<Dataset> inputs() {
        return inputs;
    }

    public List<Dataset> outputs() {
        return outputs;
    }

    public String jobType() {
        return jobType;
    }

    @Override
    public String toString() {
        return "ConnectorLineage{inputs=" + inputs + ", outputs=" + outputs
            + ", jobType='" + jobType + "'}";
    }
}
