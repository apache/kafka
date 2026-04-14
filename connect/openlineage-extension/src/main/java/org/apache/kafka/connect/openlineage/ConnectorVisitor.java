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

import java.util.Map;

/**
 * A strategy that inspects a connector's configuration and extracts its
 * input/output datasets.  Implementations should match on
 * {@code connector.class} to decide whether they apply.
 */
public interface ConnectorVisitor {

    /**
     * Returns {@code true} if this visitor knows how to extract lineage from
     * the given connector configuration.
     *
     * @param config the connector configuration
     * @return {@code true} if this visitor can handle the connector
     */
    boolean matches(Map<String, String> config);

    /**
     * Extract input and output datasets from the connector configuration.
     *
     * @param config the connector configuration
     * @return the extracted lineage information
     */
    ConnectorLineage visit(Map<String, String> config);
}
