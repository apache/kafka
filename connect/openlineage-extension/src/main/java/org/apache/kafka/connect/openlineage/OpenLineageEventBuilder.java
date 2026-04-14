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

import org.apache.kafka.common.utils.AppInfoParser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Builds OpenLineage {@code RunEvent} JSON documents that conform to the
 * <a href="https://openlineage.io/spec/2-0-2/OpenLineage.json">OpenLineage
 * spec</a>.  All JSON is constructed via Jackson {@link ObjectMapper} to
 * avoid pulling in the heavy {@code openlineage-java} client library.
 */
public final class OpenLineageEventBuilder {

    private static final String SCHEMA_URL =
        "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
    private static final String PRODUCER =
        "https://github.com/apache/kafka/tree/trunk/connect/openlineage-extension";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OpenLineageEventBuilder() {
        // utility class
    }

    /**
     * Event types as defined by the OpenLineage spec.
     */
    public enum EventType {
        START,
        RUNNING,
        COMPLETE,
        FAIL,
        ABORT
    }

    /**
     * Build a complete OpenLineage RunEvent JSON string.
     *
     * @param eventType    the lifecycle event type
     * @param runId        the UUID for this run
     * @param namespace    the job namespace (e.g. {@code kafka-connect})
     * @param jobName      the job name (typically the connector name)
     * @param jobType      the connector job type (e.g. {@code JDBC_SOURCE})
     * @param inputs       input datasets
     * @param outputs      output datasets
     * @param errorMessage optional error message for FAIL events; may be
     *                     {@code null}
     * @return a JSON string representing the RunEvent
     */
    public static String buildRunEvent(EventType eventType,
                                       UUID runId,
                                       String namespace,
                                       String jobName,
                                       String jobType,
                                       List<ConnectorLineage.Dataset> inputs,
                                       List<ConnectorLineage.Dataset> outputs,
                                       String errorMessage) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("eventTime", ZonedDateTime.now(ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        root.put("producer", PRODUCER);
        root.put("schemaURL", SCHEMA_URL);
        root.put("eventType", eventType.name());

        // -- run --
        ObjectNode run = MAPPER.createObjectNode();
        run.put("runId", runId.toString());

        ObjectNode runFacets = MAPPER.createObjectNode();

        // processing_engine facet
        ObjectNode processingEngine = MAPPER.createObjectNode();
        processingEngine.put("_producer", PRODUCER);
        processingEngine.put("_schemaURL",
            "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet");
        processingEngine.put("version", AppInfoParser.getVersion());
        processingEngine.put("name", "Apache Kafka Connect");
        runFacets.set("processing_engine", processingEngine);

        // errorMessage facet for FAIL events
        if (eventType == EventType.FAIL && errorMessage != null) {
            ObjectNode errorFacet = MAPPER.createObjectNode();
            errorFacet.put("_producer", PRODUCER);
            errorFacet.put("_schemaURL",
                "https://openlineage.io/spec/facets/1-1-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet");
            errorFacet.put("message", truncate(errorMessage, 4096));
            errorFacet.put("programmingLanguage", "JAVA");
            runFacets.set("errorMessage", errorFacet);
        }

        run.set("facets", runFacets);
        root.set("run", run);

        // -- job --
        ObjectNode job = MAPPER.createObjectNode();
        job.put("namespace", namespace);
        job.put("name", jobName);

        ObjectNode jobFacets = MAPPER.createObjectNode();
        ObjectNode jobTypeFacet = MAPPER.createObjectNode();
        jobTypeFacet.put("_producer", PRODUCER);
        jobTypeFacet.put("_schemaURL",
            "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet");
        jobTypeFacet.put("processingType", "STREAMING");
        jobTypeFacet.put("integration", "KAFKA_CONNECT");
        jobTypeFacet.put("jobType", jobType);
        jobFacets.set("jobType", jobTypeFacet);

        job.set("facets", jobFacets);
        root.set("job", job);

        // -- inputs --
        root.set("inputs", buildDatasets(inputs));

        // -- outputs --
        root.set("outputs", buildDatasets(outputs));

        return root.toString();
    }

    private static ArrayNode buildDatasets(List<ConnectorLineage.Dataset> datasets) {
        ArrayNode array = MAPPER.createArrayNode();
        if (datasets != null) {
            for (ConnectorLineage.Dataset ds : datasets) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("namespace", ds.namespace());
                node.put("name", ds.name());
                node.set("facets", MAPPER.createObjectNode());
                array.add(node);
            }
        }
        return array;
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) {
            return null;
        }
        return s.length() <= maxLen ? s : s.substring(0, maxLen);
    }
}
