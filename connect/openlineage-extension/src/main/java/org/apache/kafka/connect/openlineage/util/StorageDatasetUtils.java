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

package org.apache.kafka.connect.openlineage.util;

import org.apache.kafka.connect.openlineage.ConnectorLineage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility for building object-store / filesystem output datasets for sink
 * connectors (S3, GCS, Azure Blob, HDFS).
 *
 * <p>Per the OpenLineage naming spec the dataset name is the object key / path.
 * These connectors write each topic under {@code {topics.dir}/{topic}}, so this
 * produces one dataset per topic named {@code {topicsDir}/{topic}} rather than a
 * single coarse {@code {topicsDir}} entry that would collapse every topic into
 * one node.  When no concrete topics are configured (e.g. {@code topics.regex}),
 * a single {@code {topicsDir}} entry is returned.
 */
public final class StorageDatasetUtils {

    private StorageDatasetUtils() {
        // utility class
    }

    /**
     * Build per-topic path datasets under the given storage namespace.
     *
     * @param namespace the OpenLineage namespace (e.g. {@code s3://bucket})
     * @param topicsDir the connector's {@code topics.dir} root
     * @param config    the connector configuration (for the topic list)
     * @return one dataset per topic, or a single {@code topicsDir} entry
     */
    public static List<ConnectorLineage.Dataset> pathDatasets(
            String namespace, String topicsDir, Map<String, String> config) {
        List<String> topics = KafkaDatasetUtils.parseTopics(config);
        if (topics.isEmpty()) {
            return Collections.singletonList(
                new ConnectorLineage.Dataset(namespace, topicsDir));
        }
        List<ConnectorLineage.Dataset> datasets = new ArrayList<>();
        for (String topic : topics) {
            datasets.add(new ConnectorLineage.Dataset(namespace, topicsDir + "/" + topic));
        }
        return datasets;
    }
}
