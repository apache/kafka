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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;
import static org.apache.kafka.common.protocol.types.Type.STRING;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;

public class Protocol {

    public static final Schema REQUEST_HEADER = new Schema(new Field("api_key", INT16, "The id of the request type."),
                                                           new Field("api_version", INT16, "The version of the API."),
                                                           new Field("correlation_id",
                                                                     INT32,
                                                                     "A user-supplied integer value that will be passed back with the response"),
                                                           new Field("client_id",
                                                                     NULLABLE_STRING,
                                                                     "A user specified identifier for the client making the request.",
                                                                     ""));

    public static final Schema RESPONSE_HEADER = new Schema(new Field("correlation_id",
                                                                      INT32,
                                                                      "The user-supplied value passed in with the request"));

    /* Metadata api */

    public static final Schema METADATA_REQUEST_V0 = new Schema(new Field("topics",
                                                                          new ArrayOf(STRING),
                                                                          "An array of topics to fetch metadata for. If no topics are specified fetch metadata for all topics."));

    public static final Schema METADATA_REQUEST_V1 = new Schema(new Field("topics",
                                                                          ArrayOf.nullable(STRING),
                                                                          "An array of topics to fetch metadata for. If the topics array is null fetch metadata for all topics."));

    /* The v2 metadata request is the same as v1. An additional field for cluster id has been added to the v2 metadata response */
    public static final Schema METADATA_REQUEST_V2 = METADATA_REQUEST_V1;

    /* The v3 metadata request is the same as v1 and v2. An additional field for throttle time has been added to the v3 metadata response */
    public static final Schema METADATA_REQUEST_V3 = METADATA_REQUEST_V2;

    public static final Schema METADATA_BROKER_V0 = new Schema(new Field("node_id", INT32, "The broker id."),
                                                   new Field("host", STRING, "The hostname of the broker."),
                                                   new Field("port", INT32,
                                                             "The port on which the broker accepts requests."));

    public static final Schema PARTITION_METADATA_V0 = new Schema(new Field("partition_error_code",
                                                                            INT16,
                                                                            "The error code for the partition, if any."),
                                                                  new Field("partition_id",
                                                                            INT32,
                                                                            "The id of the partition."),
                                                                  new Field("leader",
                                                                            INT32,
                                                                            "The id of the broker acting as leader for this partition."),
                                                                  new Field("replicas",
                                                                            new ArrayOf(INT32),
                                                                            "The set of all nodes that host this partition."),
                                                                  new Field("isr",
                                                                            new ArrayOf(INT32),
                                                                            "The set of nodes that are in sync with the leader for this partition."));

    public static final Schema TOPIC_METADATA_V0 = new Schema(new Field("topic_error_code",
                                                                        INT16,
                                                                        "The error code for the given topic."),
                                                              new Field("topic", STRING, "The name of the topic"),
                                                              new Field("partition_metadata",
                                                                        new ArrayOf(PARTITION_METADATA_V0),
                                                                        "Metadata for each partition of the topic."));

    public static final Schema METADATA_RESPONSE_V0 = new Schema(new Field("brokers",
                                                                           new ArrayOf(METADATA_BROKER_V0),
                                                                           "Host and port information for all brokers."),
                                                                 new Field("topic_metadata",
                                                                           new ArrayOf(TOPIC_METADATA_V0)));

    public static final Schema METADATA_BROKER_V1 = new Schema(new Field("node_id", INT32, "The broker id."),
                                                      new Field("host", STRING, "The hostname of the broker."),
                                                      new Field("port", INT32,
                                                        "The port on which the broker accepts requests."),
                                                      new Field("rack", NULLABLE_STRING, "The rack of the broker."));

    public static final Schema PARTITION_METADATA_V1 = PARTITION_METADATA_V0;

    public static final Schema TOPIC_METADATA_V1 = new Schema(new Field("topic_error_code", INT16, "The error code for the given topic."),
                                                              new Field("topic", STRING, "The name of the topic"),
                                                              new Field("is_internal", BOOLEAN,
                                                                  "Indicates if the topic is considered a Kafka internal topic"),
                                                              new Field("partition_metadata", new ArrayOf(PARTITION_METADATA_V1),
                                                                "Metadata for each partition of the topic."));

    public static final Schema METADATA_RESPONSE_V1 = new Schema(new Field("brokers", new ArrayOf(METADATA_BROKER_V1),
                                                                    "Host and port information for all brokers."),
                                                                 new Field("controller_id", INT32,
                                                                     "The broker id of the controller broker."),
                                                                 new Field("topic_metadata", new ArrayOf(TOPIC_METADATA_V1)));

    public static final Schema METADATA_RESPONSE_V2 = new Schema(new Field("brokers", new ArrayOf(METADATA_BROKER_V1),
                                                                    "Host and port information for all brokers."),
                                                                 new Field("cluster_id", NULLABLE_STRING,
                                                                     "The cluster id that this broker belongs to."),
                                                                 new Field("controller_id", INT32,
                                                                     "The broker id of the controller broker."),
                                                                 new Field("topic_metadata", new ArrayOf(TOPIC_METADATA_V1)));

    public static final Schema METADATA_RESPONSE_V3 = new Schema(
         newThrottleTimeField(),
         new Field("brokers", new ArrayOf(METADATA_BROKER_V1),
            "Host and port information for all brokers."),
         new Field("cluster_id", NULLABLE_STRING,
             "The cluster id that this broker belongs to."),
         new Field("controller_id", INT32,
             "The broker id of the controller broker."),
         new Field("topic_metadata", new ArrayOf(TOPIC_METADATA_V1)));

    public static final Schema[] METADATA_REQUEST = {METADATA_REQUEST_V0, METADATA_REQUEST_V1, METADATA_REQUEST_V2, METADATA_REQUEST_V3};
    public static final Schema[] METADATA_RESPONSE = {METADATA_RESPONSE_V0, METADATA_RESPONSE_V1, METADATA_RESPONSE_V2, METADATA_RESPONSE_V3};

    /* Produce api */

    public static final Schema TOPIC_PRODUCE_DATA_V0 = new Schema(new Field("topic", STRING),
                                                                  new Field("data", new ArrayOf(new Schema(new Field("partition", INT32),
                                                                                                     new Field("record_set", RECORDS)))));

    public static final Schema PRODUCE_REQUEST_V0 = new Schema(new Field("acks",
                                                                   INT16,
                                                                   "The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR."),
                                                               new Field("timeout", INT32, "The time to await a response in ms."),
                                                               new Field("topic_data", new ArrayOf(TOPIC_PRODUCE_DATA_V0)));

    public static final Schema PRODUCE_RESPONSE_V0 = new Schema(new Field("responses",
                                                                    new ArrayOf(new Schema(new Field("topic", STRING),
                                                                                           new Field("partition_responses",
                                                                                                     new ArrayOf(new Schema(new Field("partition",
                                                                                                                                      INT32),
                                                                                                                            new Field("error_code",
                                                                                                                                      INT16),
                                                                                                                            new Field("base_offset",
                                                                                                                                      INT64))))))));
    /**
     * The body of PRODUCE_REQUEST_V1 is the same as PRODUCE_REQUEST_V0.
     * The version number is bumped up to indicate that the client supports quota throttle time field in the response.
     */
    public static final Schema PRODUCE_REQUEST_V1 = PRODUCE_REQUEST_V0;
    /**
     * The body of PRODUCE_REQUEST_V2 is the same as PRODUCE_REQUEST_V1.
     * The version number is bumped up to indicate that message format V1 is used which has relative offset and
     * timestamp.
     */
    public static final Schema PRODUCE_REQUEST_V2 = PRODUCE_REQUEST_V1;

    // Produce request V3 adds the transactional id which is used for authorization when attempting to write
    // transactional data. This version also adds support for message format V2.
    public static final Schema PRODUCE_REQUEST_V3 = new Schema(
            new Field("transactional_id",
                    NULLABLE_STRING,
                    "The transactional ID of the producer. This is used to authorize transaction produce requests. " +
                    "This can be null for non-transactional producers."),
            new Field("acks",
                    INT16,
                    "The number of acknowledgments the producer requires the leader to have received before " +
                    "considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader " +
                    "and -1 for the full ISR."),
            new Field("timeout", INT32, "The time to await a response in ms."),
            new Field("topic_data", new ArrayOf(TOPIC_PRODUCE_DATA_V0)));

    public static final Schema PRODUCE_RESPONSE_V1 = new Schema(new Field("responses",
                                                                          new ArrayOf(new Schema(new Field("topic", STRING),
                                                                                                 new Field("partition_responses",
                                                                                                           new ArrayOf(new Schema(new Field("partition",
                                                                                                                                            INT32),
                                                                                                                                  new Field("error_code",
                                                                                                                                            INT16),
                                                                                                                                  new Field("base_offset",
                                                                                                                                            INT64))))))),
                                                                newThrottleTimeField());
    /**
     * PRODUCE_RESPONSE_V2 added a timestamp field in the per partition response status.
     * The timestamp is log append time if the topic is configured to use log append time. Or it is NoTimestamp when create
     * time is used for the topic.
     */
    public static final Schema PRODUCE_RESPONSE_V2 = new Schema(new Field("responses",
                                                                new ArrayOf(new Schema(new Field("topic", STRING),
                                                                                       new Field("partition_responses",
                                                                                       new ArrayOf(new Schema(new Field("partition",
                                                                                                                        INT32),
                                                                                                              new Field("error_code",
                                                                                                                        INT16),
                                                                                                              new Field("base_offset",
                                                                                                                        INT64),
                                                                                                              new Field("log_append_time",
                                                                                                                        INT64,
                                                                                                                        "The timestamp returned by broker after appending the messages. " +
                                                                                                                            "If CreateTime is used for the topic, the timestamp will be -1. " +
                                                                                                                            "If LogAppendTime is used for the topic, the timestamp will be " +
                                                                                                                            "the broker local time when the messages are appended."))))))),
                                                                newThrottleTimeField());
    public static final Schema PRODUCE_RESPONSE_V3 = PRODUCE_RESPONSE_V2;

    public static final Schema[] PRODUCE_REQUEST = {PRODUCE_REQUEST_V0, PRODUCE_REQUEST_V1, PRODUCE_REQUEST_V2, PRODUCE_REQUEST_V3};
    public static final Schema[] PRODUCE_RESPONSE = {PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2, PRODUCE_RESPONSE_V3};

    /* Offset commit api */
    public static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                         INT32,
                                                                                         "Topic partition id."),
                                                                               new Field("offset",
                                                                                         INT64,
                                                                                         "Message offset to be committed."),
                                                                               new Field("metadata",
                                                                                         NULLABLE_STRING,
                                                                                         "Any associated metadata the client wants to keep."));

    public static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V1 = new Schema(new Field("partition",
                                                                                         INT32,
                                                                                         "Topic partition id."),
                                                                               new Field("offset",
                                                                                         INT64,
                                                                                         "Message offset to be committed."),
                                                                               new Field("timestamp",
                                                                                         INT64,
                                                                                         "Timestamp of the commit"),
                                                                               new Field("metadata",
                                                                                         NULLABLE_STRING,
                                                                                         "Any associated metadata the client wants to keep."));

    public static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V2 = new Schema(new Field("partition",
                                                                                         INT32,
                                                                                         "Topic partition id."),
                                                                               new Field("offset",
                                                                                         INT64,
                                                                                         "Message offset to be committed."),
                                                                               new Field("metadata",
                                                                                         NULLABLE_STRING,
                                                                                         "Any associated metadata the client wants to keep."));

    public static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V0 = new Schema(new Field("topic",
                                                                                     STRING,
                                                                                     "Topic to commit."),
                                                                           new Field("partitions",
                                                                                     new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V0),
                                                                                     "Partitions to commit offsets."));

    public static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V1 = new Schema(new Field("topic",
                                                                                     STRING,
                                                                                     "Topic to commit."),
                                                                           new Field("partitions",
                                                                                     new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V1),
                                                                                     "Partitions to commit offsets."));

    public static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V2 = new Schema(new Field("topic",
                                                                                     STRING,
                                                                                     "Topic to commit."),
                                                                           new Field("partitions",
                                                                                     new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V2),
                                                                                     "Partitions to commit offsets."));

    public static final Schema OFFSET_COMMIT_REQUEST_V0 = new Schema(new Field("group_id",
                                                                               STRING,
                                                                               "The group id."),
                                                                     new Field("topics",
                                                                               new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V0),
                                                                               "Topics to commit offsets."));

    public static final Schema OFFSET_COMMIT_REQUEST_V1 = new Schema(new Field("group_id",
                                                                               STRING,
                                                                               "The group id."),
                                                                     new Field("group_generation_id",
                                                                               INT32,
                                                                               "The generation of the group."),
                                                                     new Field("member_id",
                                                                               STRING,
                                                                               "The member id assigned by the group coordinator."),
                                                                     new Field("topics",
                                                                               new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V1),
                                                                               "Topics to commit offsets."));

    public static final Schema OFFSET_COMMIT_REQUEST_V2 = new Schema(new Field("group_id",
                                                                               STRING,
                                                                               "The group id."),
                                                                     new Field("group_generation_id",
                                                                               INT32,
                                                                               "The generation of the consumer group."),
                                                                     new Field("member_id",
                                                                               STRING,
                                                                               "The consumer id assigned by the group coordinator."),
                                                                     new Field("retention_time",
                                                                               INT64,
                                                                               "Time period in ms to retain the offset."),
                                                                     new Field("topics",
                                                                               new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V2),
                                                                               "Topics to commit offsets."));

    /* v3 request is same as v2. Throttle time has been added to response */
    public static final Schema OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2;

    public static final Schema OFFSET_COMMIT_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                                          INT32,
                                                                                          "Topic partition id."),
                                                                                new Field("error_code",
                                                                                          INT16));

    public static final Schema OFFSET_COMMIT_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                            new Field("partition_responses",
                                                                                      new ArrayOf(OFFSET_COMMIT_RESPONSE_PARTITION_V0)));

    public static final Schema OFFSET_COMMIT_RESPONSE_V0 = new Schema(new Field("responses",
                                                                                new ArrayOf(OFFSET_COMMIT_RESPONSE_TOPIC_V0)));

    public static final Schema[] OFFSET_COMMIT_REQUEST = {OFFSET_COMMIT_REQUEST_V0, OFFSET_COMMIT_REQUEST_V1, OFFSET_COMMIT_REQUEST_V2, OFFSET_COMMIT_REQUEST_V3};

    /* The response types for V0, V1 and V2 of OFFSET_COMMIT_REQUEST are the same. */
    public static final Schema OFFSET_COMMIT_RESPONSE_V1 = OFFSET_COMMIT_RESPONSE_V0;
    public static final Schema OFFSET_COMMIT_RESPONSE_V2 = OFFSET_COMMIT_RESPONSE_V0;

    public static final Schema OFFSET_COMMIT_RESPONSE_V3 = new Schema(
            newThrottleTimeField(),
            new Field("responses",
                       new ArrayOf(OFFSET_COMMIT_RESPONSE_TOPIC_V0)));

    public static final Schema[] OFFSET_COMMIT_RESPONSE = {OFFSET_COMMIT_RESPONSE_V0, OFFSET_COMMIT_RESPONSE_V1, OFFSET_COMMIT_RESPONSE_V2, OFFSET_COMMIT_RESPONSE_V3};

    /* Offset fetch api */

    /*
     * Wire formats of version 0 and 1 are the same, but with different functionality.
     * Wire format of version 2 is similar to version 1, with the exception of
     * - accepting 'null' as list of topics
     * - returning a top level error code
     * Version 0 will read the offsets from ZK.
     * Version 1 will read the offsets from Kafka.
     * Version 2 will read the offsets from Kafka, and returns all associated topic partition offsets if
     * a 'null' is passed instead of a list of specific topic partitions. It also returns a top level error code
     * for group or coordinator level errors.
     */
    public static final Schema OFFSET_FETCH_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                        INT32,
                                                                                        "Topic partition id."));

    public static final Schema OFFSET_FETCH_REQUEST_TOPIC_V0 = new Schema(new Field("topic",
                                                                                    STRING,
                                                                                    "Topic to fetch offset."),
                                                                          new Field("partitions",
                                                                                    new ArrayOf(OFFSET_FETCH_REQUEST_PARTITION_V0),
                                                                                    "Partitions to fetch offsets."));

    public static final Schema OFFSET_FETCH_REQUEST_V0 = new Schema(new Field("group_id",
                                                                              STRING,
                                                                              "The consumer group id."),
                                                                    new Field("topics",
                                                                              new ArrayOf(OFFSET_FETCH_REQUEST_TOPIC_V0),
                                                                              "Topics to fetch offsets."));

    public static final Schema OFFSET_FETCH_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                                         INT32,
                                                                                         "Topic partition id."),
                                                                               new Field("offset",
                                                                                         INT64,
                                                                                         "Last committed message offset."),
                                                                               new Field("metadata",
                                                                                         NULLABLE_STRING,
                                                                                         "Any associated metadata the client wants to keep."),
                                                                               new Field("error_code", INT16));

    public static final Schema OFFSET_FETCH_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                           new Field("partition_responses",
                                                                                     new ArrayOf(OFFSET_FETCH_RESPONSE_PARTITION_V0)));

    public static final Schema OFFSET_FETCH_RESPONSE_V0 = new Schema(new Field("responses",
                                                                               new ArrayOf(OFFSET_FETCH_RESPONSE_TOPIC_V0)));

    public static final Schema OFFSET_FETCH_REQUEST_V1 = OFFSET_FETCH_REQUEST_V0;
    public static final Schema OFFSET_FETCH_RESPONSE_V1 = OFFSET_FETCH_RESPONSE_V0;

    public static final Schema OFFSET_FETCH_REQUEST_V2 = new Schema(new Field("group_id",
                                                                              STRING,
                                                                              "The consumer group id."),
                                                                              new Field("topics",
                                                                                        ArrayOf.nullable(OFFSET_FETCH_REQUEST_TOPIC_V0),
                                                                                        "Topics to fetch offsets. If the topic array is null fetch offsets for all topics."));

    public static final Schema OFFSET_FETCH_RESPONSE_V2 = new Schema(new Field("responses",
                                                                               new ArrayOf(OFFSET_FETCH_RESPONSE_TOPIC_V0)),
                                                                     new Field("error_code",
                                                                               INT16));

    /* v3 request is the same as v2. Throttle time has been added to v3 response */
    public static final Schema OFFSET_FETCH_REQUEST_V3 = OFFSET_FETCH_REQUEST_V2;
    public static final Schema OFFSET_FETCH_RESPONSE_V3 = new Schema(
            newThrottleTimeField(),
            new Field("responses",
                    new ArrayOf(OFFSET_FETCH_RESPONSE_TOPIC_V0)),
            new Field("error_code",
                    INT16));

    public static final Schema[] OFFSET_FETCH_REQUEST = {OFFSET_FETCH_REQUEST_V0, OFFSET_FETCH_REQUEST_V1, OFFSET_FETCH_REQUEST_V2, OFFSET_FETCH_REQUEST_V3};
    public static final Schema[] OFFSET_FETCH_RESPONSE = {OFFSET_FETCH_RESPONSE_V0, OFFSET_FETCH_RESPONSE_V1, OFFSET_FETCH_RESPONSE_V2, OFFSET_FETCH_RESPONSE_V3};

    /* List offset api */
    public static final Schema LIST_OFFSET_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                       INT32,
                                                                                       "Topic partition id."),
                                                                             new Field("timestamp", INT64, "Timestamp."),
                                                                             new Field("max_num_offsets",
                                                                                       INT32,
                                                                                       "Maximum offsets to return."));
    public static final Schema LIST_OFFSET_REQUEST_PARTITION_V1 = new Schema(new Field("partition",
                                                                                       INT32,
                                                                                       "Topic partition id."),
                                                                             new Field("timestamp",
                                                                                       INT64,
                                                                                       "The target timestamp for the partition."));

    public static final Schema LIST_OFFSET_REQUEST_TOPIC_V0 = new Schema(new Field("topic",
                                                                                   STRING,
                                                                                   "Topic to list offset."),
                                                                         new Field("partitions",
                                                                                   new ArrayOf(LIST_OFFSET_REQUEST_PARTITION_V0),
                                                                                   "Partitions to list offset."));
    public static final Schema LIST_OFFSET_REQUEST_TOPIC_V1 = new Schema(new Field("topic",
                                                                                   STRING,
                                                                                   "Topic to list offset."),
                                                                         new Field("partitions",
                                                                                   new ArrayOf(LIST_OFFSET_REQUEST_PARTITION_V1),
                                                                                   "Partitions to list offset."));

    public static final Schema LIST_OFFSET_REQUEST_V0 = new Schema(new Field("replica_id",
                                                                             INT32,
                                                                             "Broker id of the follower. For normal consumers, use -1."),
                                                                   new Field("topics",
                                                                             new ArrayOf(LIST_OFFSET_REQUEST_TOPIC_V0),
                                                                             "Topics to list offsets."));
    public static final Schema LIST_OFFSET_REQUEST_V1 = new Schema(new Field("replica_id",
                                                                             INT32,
                                                                             "Broker id of the follower. For normal consumers, use -1."),
                                                                   new Field("topics",
                                                                             new ArrayOf(LIST_OFFSET_REQUEST_TOPIC_V1),
                                                                             "Topics to list offsets."));

    public static final Schema LIST_OFFSET_REQUEST_V2 = new Schema(
            new Field("replica_id",
                    INT32,
                    "Broker id of the follower. For normal consumers, use -1."),
            new Field("isolation_level",
                    INT8,
                    "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                            "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                            "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                            "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                            "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                            "consumers to discard ABORTED transactional records"),
            new Field("topics",
                    new ArrayOf(LIST_OFFSET_REQUEST_TOPIC_V1),
                    "Topics to list offsets."));;

    public static final Schema LIST_OFFSET_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                                        INT32,
                                                                                        "Topic partition id."),
                                                                              new Field("error_code", INT16),
                                                                              new Field("offsets",
                                                                                        new ArrayOf(INT64),
                                                                                        "A list of offsets."));

    public static final Schema LIST_OFFSET_RESPONSE_PARTITION_V1 = new Schema(new Field("partition",
                                                                                        INT32,
                                                                                        "Topic partition id."),
                                                                              new Field("error_code", INT16),
                                                                              new Field("timestamp",
                                                                                        INT64,
                                                                                        "The timestamp associated with the returned offset"),
                                                                              new Field("offset",
                                                                                        INT64,
                                                                                        "offset found"));

    public static final Schema LIST_OFFSET_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                          new Field("partition_responses",
                                                                                    new ArrayOf(LIST_OFFSET_RESPONSE_PARTITION_V0)));

    public static final Schema LIST_OFFSET_RESPONSE_TOPIC_V1 = new Schema(new Field("topic", STRING),
                                                                          new Field("partition_responses",
                                                                                    new ArrayOf(LIST_OFFSET_RESPONSE_PARTITION_V1)));

    public static final Schema LIST_OFFSET_RESPONSE_V0 = new Schema(new Field("responses",
                                                                              new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V0)));

    public static final Schema LIST_OFFSET_RESPONSE_V1 = new Schema(new Field("responses",
                                                                              new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V1)));
    public static final Schema LIST_OFFSET_RESPONSE_V2 = new Schema(
            newThrottleTimeField(),
            new Field("responses",
                    new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V1)));

    public static final Schema[] LIST_OFFSET_REQUEST = {LIST_OFFSET_REQUEST_V0, LIST_OFFSET_REQUEST_V1, LIST_OFFSET_REQUEST_V2};
    public static final Schema[] LIST_OFFSET_RESPONSE = {LIST_OFFSET_RESPONSE_V0, LIST_OFFSET_RESPONSE_V1, LIST_OFFSET_RESPONSE_V2};

    /* Fetch api */
    public static final Schema FETCH_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                 INT32,
                                                                                 "Topic partition id."),
                                                                       new Field("fetch_offset",
                                                                                 INT64,
                                                                                 "Message offset."),
                                                                       new Field("max_bytes",
                                                                                 INT32,
                                                                                 "Maximum bytes to fetch."));

    // FETCH_REQUEST_PARTITION_V1 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    public static final Schema FETCH_REQUEST_PARTITION_V5 = new Schema(new Field("partition",
                                                                                 INT32,
                                                                                 "Topic partition id."),
                                                                       new Field("fetch_offset",
                                                                                 INT64,
                                                                                 "Message offset."),
                                                                       new Field("log_start_offset",
                                                                                 INT64,
                                                                                 "Earliest available offset of the follower replica. " +
                                                                                 "The field is only used when request is sent by follower. "),
                                                                       new Field("max_bytes",
                                                                                 INT32,
                                                                                 "Maximum bytes to fetch."));

    public static final Schema FETCH_REQUEST_TOPIC_V0 = new Schema(new Field("topic", STRING, "Topic to fetch."),
                                                                   new Field("partitions",
                                                                             new ArrayOf(FETCH_REQUEST_PARTITION_V0),
                                                                             "Partitions to fetch."));

    public static final Schema FETCH_REQUEST_TOPIC_V5 = new Schema(new Field("topic", STRING, "Topic to fetch."),
                                                                   new Field("partitions",
                                                                             new ArrayOf(FETCH_REQUEST_PARTITION_V5),
                                                                             "Partitions to fetch."));

    public static final Schema FETCH_REQUEST_V0 = new Schema(new Field("replica_id",
                                                                       INT32,
                                                                       "Broker id of the follower. For normal consumers, use -1."),
                                                             new Field("max_wait_time",
                                                                       INT32,
                                                                       "Maximum time in ms to wait for the response."),
                                                             new Field("min_bytes",
                                                                       INT32,
                                                                       "Minimum bytes to accumulate in the response."),
                                                             new Field("topics",
                                                                       new ArrayOf(FETCH_REQUEST_TOPIC_V0),
                                                                       "Topics to fetch."));

    // The V1 Fetch Request body is the same as V0.
    // Only the version number is incremented to indicate a newer client
    public static final Schema FETCH_REQUEST_V1 = FETCH_REQUEST_V0;
    // The V2 Fetch Request body is the same as V1.
    // Only the version number is incremented to indicate the client support message format V1 which uses
    // relative offset and has timestamp.
    public static final Schema FETCH_REQUEST_V2 = FETCH_REQUEST_V1;
    // Fetch Request V3 added top level max_bytes field - the total size of partition data to accumulate in response.
    // The partition ordering is now relevant - partitions will be processed in order they appear in request.
    public static final Schema FETCH_REQUEST_V3 = new Schema(new Field("replica_id",
                                                                       INT32,
                                                                       "Broker id of the follower. For normal consumers, use -1."),
                                                             new Field("max_wait_time",
                                                                       INT32,
                                                                       "Maximum time in ms to wait for the response."),
                                                             new Field("min_bytes",
                                                                       INT32,
                                                                       "Minimum bytes to accumulate in the response."),
                                                             new Field("max_bytes",
                                                                       INT32,
                                                                       "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                                                                       "if the first message in the first non-empty partition of the fetch is larger than this " +
                                                                       "value, the message will still be returned to ensure that progress can be made."),
                                                             new Field("topics",
                                                                       new ArrayOf(FETCH_REQUEST_TOPIC_V0),
                                                                       "Topics to fetch in the order provided."));

    // The V4 Fetch Request adds the fetch isolation level and exposes magic v2 (via the response).
    public static final Schema FETCH_REQUEST_V4 = new Schema(
            new Field("replica_id",
                    INT32,
                    "Broker id of the follower. For normal consumers, use -1."),
            new Field("max_wait_time",
                    INT32,
                    "Maximum time in ms to wait for the response."),
            new Field("min_bytes",
                    INT32,
                    "Minimum bytes to accumulate in the response."),
            new Field("max_bytes",
                    INT32,
                    "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field("isolation_level",
                    INT8,
                    "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                    "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                     "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                     "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                     "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                     "consumers to discard ABORTED transactional records"),
            new Field("topics",
                    new ArrayOf(FETCH_REQUEST_TOPIC_V0),
                    "Topics to fetch in the order provided."));

    // FETCH_REQUEST_V5 added a per-partition log_start_offset field - the earliest available offset of partition data that can be consumed.
    public static final Schema FETCH_REQUEST_V5 = new Schema(
            new Field("replica_id",
                    INT32,
                    "Broker id of the follower. For normal consumers, use -1."),
            new Field("max_wait_time",
                    INT32,
                    "Maximum time in ms to wait for the response."),
            new Field("min_bytes",
                    INT32,
                    "Minimum bytes to accumulate in the response."),
            new Field("max_bytes",
                    INT32,
                    "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field("isolation_level",
                    INT8,
                    "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                    "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                     "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                     "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                     "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                     "consumers to discard ABORTED transactional records"),
            new Field("topics",
                    new ArrayOf(FETCH_REQUEST_TOPIC_V5),
                    "Topics to fetch in the order provided."));

    public static final Schema FETCH_RESPONSE_PARTITION_HEADER_V0 = new Schema(new Field("partition",
                                                                                         INT32,
                                                                                         "Topic partition id."),
                                                                               new Field("error_code", INT16),
                                                                               new Field("high_watermark",
                                                                                         INT64,
                                                                                         "Last committed offset."));
    public static final Schema FETCH_RESPONSE_PARTITION_V0 = new Schema(new Field("partition_header", FETCH_RESPONSE_PARTITION_HEADER_V0),
                                                                        new Field("record_set", RECORDS));

    public static final Schema FETCH_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                    new Field("partition_responses",
                                                                              new ArrayOf(FETCH_RESPONSE_PARTITION_V0)));

    public static final Schema FETCH_RESPONSE_V0 = new Schema(new Field("responses",
                                                                        new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));

    public static final Schema FETCH_RESPONSE_V1 = new Schema(newThrottleTimeField(),
                                                              new Field("responses",
                                                                      new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));
    // Even though fetch response v2 has the same protocol as v1, the record set in the response is different. In v1,
    // record set only includes messages of v0 (magic byte 0). In v2, record set can include messages of v0 and v1
    // (magic byte 0 and 1). For details, see ByteBufferMessageSet.
    public static final Schema FETCH_RESPONSE_V2 = FETCH_RESPONSE_V1;
    public static final Schema FETCH_RESPONSE_V3 = FETCH_RESPONSE_V2;


    // The v4 Fetch Response adds features for transactional consumption (the aborted transaction list and the
    // last stable offset). It also exposes messages with magic v2 (along with older formats).
    private static final Schema FETCH_RESPONSE_ABORTED_TRANSACTION_V4 = new Schema(
            new Field("producer_id", INT64, "The producer id associated with the aborted transactions"),
            new Field("first_offset", INT64, "The first offset in the aborted transaction"));

    public static final Schema FETCH_RESPONSE_ABORTED_TRANSACTION_V5 = FETCH_RESPONSE_ABORTED_TRANSACTION_V4;

    public static final Schema FETCH_RESPONSE_PARTITION_HEADER_V4 = new Schema(
            new Field("partition",
                    INT32,
                    "Topic partition id."),
            new Field("error_code", INT16),
            new Field("high_watermark",
                    INT64,
                    "Last committed offset."),
            new Field("last_stable_offset",
                    INT64,
                    "The last stable offset (or LSO) of the partition. This is the last offset such that the state " +
                    "of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
            new Field("aborted_transactions",
                    ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V4)));

    // FETCH_RESPONSE_PARTITION_HEADER_V5 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    public static final Schema FETCH_RESPONSE_PARTITION_HEADER_V5 = new Schema(
            new Field("partition",
                    INT32,
                    "Topic partition id."),
            new Field("error_code", INT16),
            new Field("high_watermark",
                    INT64,
                    "Last committed offset."),
            new Field("last_stable_offset",
                    INT64,
                    "The last stable offset (or LSO) of the partition. This is the last offset such that the state " +
                    "of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
            new Field("log_start_offset",
                    INT64,
                    "Earliest available offset."),
            new Field("aborted_transactions",
                    ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V5)));

    public static final Schema FETCH_RESPONSE_PARTITION_V4 = new Schema(
            new Field("partition_header", FETCH_RESPONSE_PARTITION_HEADER_V4),
            new Field("record_set", RECORDS));

    public static final Schema FETCH_RESPONSE_PARTITION_V5 = new Schema(
            new Field("partition_header", FETCH_RESPONSE_PARTITION_HEADER_V5),
            new Field("record_set", RECORDS));

    public static final Schema FETCH_RESPONSE_TOPIC_V4 = new Schema(
            new Field("topic", STRING),
            new Field("partition_responses", new ArrayOf(FETCH_RESPONSE_PARTITION_V4)));

    public static final Schema FETCH_RESPONSE_TOPIC_V5 = new Schema(
            new Field("topic", STRING),
            new Field("partition_responses", new ArrayOf(FETCH_RESPONSE_PARTITION_V5)));

    public static final Schema FETCH_RESPONSE_V4 = new Schema(
            newThrottleTimeField(),
            new Field("responses", new ArrayOf(FETCH_RESPONSE_TOPIC_V4)));

    public static final Schema FETCH_RESPONSE_V5 = new Schema(
            newThrottleTimeField(),
            new Field("responses", new ArrayOf(FETCH_RESPONSE_TOPIC_V5)));

    public static final Schema[] FETCH_REQUEST = {FETCH_REQUEST_V0, FETCH_REQUEST_V1, FETCH_REQUEST_V2, FETCH_REQUEST_V3, FETCH_REQUEST_V4, FETCH_REQUEST_V5};
    public static final Schema[] FETCH_RESPONSE = {FETCH_RESPONSE_V0, FETCH_RESPONSE_V1, FETCH_RESPONSE_V2, FETCH_RESPONSE_V3, FETCH_RESPONSE_V4, FETCH_RESPONSE_V5};

    /* List groups api */
    public static final Schema LIST_GROUPS_REQUEST_V0 = new Schema();

    /* v1 request is the same as v0. Throttle time has been added to response */
    public static final Schema LIST_GROUPS_REQUEST_V1 = LIST_GROUPS_REQUEST_V0;

    public static final Schema LIST_GROUPS_RESPONSE_GROUP_V0 = new Schema(new Field("group_id", STRING),
                                                                          new Field("protocol_type", STRING));
    public static final Schema LIST_GROUPS_RESPONSE_V0 = new Schema(new Field("error_code", INT16),
                                                                    new Field("groups", new ArrayOf(LIST_GROUPS_RESPONSE_GROUP_V0)));
    public static final Schema LIST_GROUPS_RESPONSE_V1 = new Schema(
            newThrottleTimeField(),
            new Field("error_code", INT16),
            new Field("groups", new ArrayOf(LIST_GROUPS_RESPONSE_GROUP_V0)));

    public static final Schema[] LIST_GROUPS_REQUEST = {LIST_GROUPS_REQUEST_V0, LIST_GROUPS_REQUEST_V1};
    public static final Schema[] LIST_GROUPS_RESPONSE = {LIST_GROUPS_RESPONSE_V0, LIST_GROUPS_RESPONSE_V1};

    /* Describe group api */
    public static final Schema DESCRIBE_GROUPS_REQUEST_V0 = new Schema(new Field("group_ids",
                                                                                 new ArrayOf(STRING),
                                                                                 "List of groupIds to request metadata for (an empty groupId array will return empty group metadata)."));

    /* v1 request is the same as v0. Throttle time has been added to response */
    public static final Schema DESCRIBE_GROUPS_REQUEST_V1 = DESCRIBE_GROUPS_REQUEST_V0;

    public static final Schema DESCRIBE_GROUPS_RESPONSE_MEMBER_V0 = new Schema(new Field("member_id",
                                                                                         STRING,
                                                                                         "The memberId assigned by the coordinator"),
                                                                               new Field("client_id",
                                                                                         STRING,
                                                                                         "The client id used in the member's latest join group request"),
                                                                               new Field("client_host",
                                                                                         STRING,
                                                                                         "The client host used in the request session corresponding to the member's join group."),
                                                                               new Field("member_metadata",
                                                                                         BYTES,
                                                                                         "The metadata corresponding to the current group protocol in use (will only be present if the group is stable)."),
                                                                               new Field("member_assignment",
                                                                                         BYTES,
                                                                                         "The current assignment provided by the group leader (will only be present if the group is stable)."));

    public static final Schema DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0 = new Schema(new Field("error_code", INT16),
                                                                                       new Field("group_id",
                                                                                                 STRING),
                                                                                       new Field("state",
                                                                                                 STRING,
                                                                                                 "The current state of the group (one of: Dead, Stable, AwaitingSync, PreparingRebalance, or empty if there is no active group)"),
                                                                                       new Field("protocol_type",
                                                                                                 STRING,
                                                                                                 "The current group protocol type (will be empty if there is no active group)"),
                                                                                       new Field("protocol",
                                                                                                 STRING,
                                                                                                 "The current group protocol (only provided if the group is Stable)"),
                                                                                       new Field("members",
                                                                                                 new ArrayOf(DESCRIBE_GROUPS_RESPONSE_MEMBER_V0),
                                                                                                 "Current group members (only provided if the group is not Dead)"));

    public static final Schema DESCRIBE_GROUPS_RESPONSE_V0 = new Schema(new Field("groups", new ArrayOf(DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0)));
    public static final Schema DESCRIBE_GROUPS_RESPONSE_V1 = new Schema(
            newThrottleTimeField(),
            new Field("groups", new ArrayOf(DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0)));

    public static final Schema[] DESCRIBE_GROUPS_REQUEST = {DESCRIBE_GROUPS_REQUEST_V0, DESCRIBE_GROUPS_REQUEST_V1};
    public static final Schema[] DESCRIBE_GROUPS_RESPONSE = {DESCRIBE_GROUPS_RESPONSE_V0, DESCRIBE_GROUPS_RESPONSE_V1};

    /* Find coordinator api */
    public static final Schema FIND_COORDINATOR_REQUEST_V0 = new Schema(
            new Field("group_id",
                    STRING,
                    "The unique group id."));

    public static final Schema FIND_COORDINATOR_REQUEST_V1 = new Schema(
            new Field("coordinator_key",
                    STRING,
                    "Id to use for finding the coordinator (for groups, this is the groupId, " +
                            "for transactional producers, this is the transactional id)"),
            new Field("coordinator_type",
                    INT8,
                    "The type of coordinator to find (0 = group, 1 = transaction)"));

    public static final Schema FIND_COORDINATOR_BROKER_V0 = new Schema(
            new Field("node_id", INT32, "The broker id."),
            new Field("host", STRING, "The hostname of the broker."),
            new Field("port", INT32,
                    "The port on which the broker accepts requests."));

    public static final Schema FIND_COORDINATOR_RESPONSE_V0 = new Schema(
            new Field("error_code", INT16),
            new Field("coordinator",
                    FIND_COORDINATOR_BROKER_V0,
                    "Host and port information for the coordinator for a consumer group."));

    public static final Schema FIND_COORDINATOR_RESPONSE_V1 = new Schema(
            newThrottleTimeField(),
            new Field("error_code", INT16),
            new Field("error_message", NULLABLE_STRING),
            new Field("coordinator",
                    FIND_COORDINATOR_BROKER_V0,
                    "Host and port information for the coordinator for a consumer group."));


    public static final Schema[] FIND_COORDINATOR_REQUEST = {FIND_COORDINATOR_REQUEST_V0, FIND_COORDINATOR_REQUEST_V1};
    public static final Schema[] FIND_COORDINATOR_RESPONSE = {FIND_COORDINATOR_RESPONSE_V0, FIND_COORDINATOR_RESPONSE_V1};

    /* Controlled shutdown api */
    public static final Schema CONTROLLED_SHUTDOWN_REQUEST_V1 = new Schema(new Field("broker_id",
                                                                                     INT32,
                                                                                     "The id of the broker for which controlled shutdown has been requested."));

    public static final Schema CONTROLLED_SHUTDOWN_PARTITION_V1 = new Schema(new Field("topic", STRING),
                                                                             new Field("partition",
                                                                                       INT32,
                                                                                       "Topic partition id."));

    public static final Schema CONTROLLED_SHUTDOWN_RESPONSE_V1 = new Schema(new Field("error_code", INT16),
                                                                            new Field("partitions_remaining",
                                                                                      new ArrayOf(CONTROLLED_SHUTDOWN_PARTITION_V1),
                                                                                      "The partitions that the broker still leads."));

    /* V0 is not supported as it would require changes to the request header not to include `clientId` */
    public static final Schema[] CONTROLLED_SHUTDOWN_REQUEST = {null, CONTROLLED_SHUTDOWN_REQUEST_V1};
    public static final Schema[] CONTROLLED_SHUTDOWN_RESPONSE = {null, CONTROLLED_SHUTDOWN_RESPONSE_V1};

    /* Join group api */
    public static final Schema JOIN_GROUP_REQUEST_PROTOCOL_V0 = new Schema(new Field("protocol_name", STRING),
                                                                           new Field("protocol_metadata", BYTES));

    public static final Schema JOIN_GROUP_REQUEST_V0 = new Schema(new Field("group_id",
                                                                            STRING,
                                                                            "The group id."),
                                                                  new Field("session_timeout",
                                                                            INT32,
                                                                            "The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms."),
                                                                  new Field("member_id",
                                                                            STRING,
                                                                            "The assigned consumer id or an empty string for a new consumer."),
                                                                  new Field("protocol_type",
                                                                            STRING,
                                                                            "Unique name for class of protocols implemented by group"),
                                                                  new Field("group_protocols",
                                                                            new ArrayOf(JOIN_GROUP_REQUEST_PROTOCOL_V0),
                                                                            "List of protocols that the member supports"));

    public static final Schema JOIN_GROUP_REQUEST_V1 = new Schema(new Field("group_id",
                                                                            STRING,
                                                                            "The group id."),
                                                                  new Field("session_timeout",
                                                                            INT32,
                                                                            "The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms."),
                                                                  new Field("rebalance_timeout",
                                                                            INT32,
                                                                            "The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group"),
                                                                  new Field("member_id",
                                                                            STRING,
                                                                            "The assigned consumer id or an empty string for a new consumer."),
                                                                  new Field("protocol_type",
                                                                            STRING,
                                                                            "Unique name for class of protocols implemented by group"),
                                                                  new Field("group_protocols",
                                                                            new ArrayOf(JOIN_GROUP_REQUEST_PROTOCOL_V0),
                                                                            "List of protocols that the member supports"));

    /* v2 request is the same as v1. Throttle time has been added to response */
    public static final Schema JOIN_GROUP_REQUEST_V2 = JOIN_GROUP_REQUEST_V1;

    public static final Schema JOIN_GROUP_RESPONSE_MEMBER_V0 = new Schema(new Field("member_id", STRING),
                                                                          new Field("member_metadata", BYTES));

    public static final Schema JOIN_GROUP_RESPONSE_V0 = new Schema(new Field("error_code", INT16),
                                                                   new Field("generation_id",
                                                                             INT32,
                                                                             "The generation of the consumer group."),
                                                                   new Field("group_protocol",
                                                                             STRING,
                                                                             "The group protocol selected by the coordinator"),
                                                                   new Field("leader_id",
                                                                             STRING,
                                                                             "The leader of the group"),
                                                                   new Field("member_id",
                                                                             STRING,
                                                                             "The consumer id assigned by the group coordinator."),
                                                                   new Field("members",
                                                                             new ArrayOf(JOIN_GROUP_RESPONSE_MEMBER_V0)));

    public static final Schema JOIN_GROUP_RESPONSE_V1 = JOIN_GROUP_RESPONSE_V0;

    public static final Schema JOIN_GROUP_RESPONSE_V2 = new Schema(
            newThrottleTimeField(),
            new Field("error_code", INT16),
            new Field("generation_id",
                      INT32,
                      "The generation of the consumer group."),
            new Field("group_protocol",
                      STRING,
                      "The group protocol selected by the coordinator"),
            new Field("leader_id",
                      STRING,
                      "The leader of the group"),
            new Field("member_id",
                      STRING,
                      "The consumer id assigned by the group coordinator."),
            new Field("members",
                      new ArrayOf(JOIN_GROUP_RESPONSE_MEMBER_V0)));


    public static final Schema[] JOIN_GROUP_REQUEST = {JOIN_GROUP_REQUEST_V0, JOIN_GROUP_REQUEST_V1, JOIN_GROUP_REQUEST_V2};
    public static final Schema[] JOIN_GROUP_RESPONSE = {JOIN_GROUP_RESPONSE_V0, JOIN_GROUP_RESPONSE_V1, JOIN_GROUP_RESPONSE_V2};

    /* SyncGroup api */
    public static final Schema SYNC_GROUP_REQUEST_MEMBER_V0 = new Schema(new Field("member_id", STRING),
                                                                         new Field("member_assignment", BYTES));
    public static final Schema SYNC_GROUP_REQUEST_V0 = new Schema(new Field("group_id", STRING),
                                                                  new Field("generation_id", INT32),
                                                                  new Field("member_id", STRING),
                                                                  new Field("group_assignment", new ArrayOf(SYNC_GROUP_REQUEST_MEMBER_V0)));

    /* v1 request is the same as v0. Throttle time has been added to response */
    public static final Schema SYNC_GROUP_REQUEST_V1 = SYNC_GROUP_REQUEST_V0;

    public static final Schema SYNC_GROUP_RESPONSE_V0 = new Schema(new Field("error_code", INT16),
                                                                   new Field("member_assignment", BYTES));
    public static final Schema SYNC_GROUP_RESPONSE_V1 = new Schema(
            newThrottleTimeField(),
            new Field("error_code", INT16),
            new Field("member_assignment", BYTES));
    public static final Schema[] SYNC_GROUP_REQUEST = {SYNC_GROUP_REQUEST_V0, SYNC_GROUP_REQUEST_V1};
    public static final Schema[] SYNC_GROUP_RESPONSE = {SYNC_GROUP_RESPONSE_V0, SYNC_GROUP_RESPONSE_V1};

    /* Heartbeat api */
    public static final Schema HEARTBEAT_REQUEST_V0 = new Schema(new Field("group_id", STRING, "The group id."),
                                                                 new Field("group_generation_id",
                                                                           INT32,
                                                                           "The generation of the group."),
                                                                 new Field("member_id",
                                                                           STRING,
                                                                           "The member id assigned by the group coordinator."));

    /* v1 request is the same as v0. Throttle time has been added to response */
    public static final Schema HEARTBEAT_REQUEST_V1 = HEARTBEAT_REQUEST_V0;

    public static final Schema HEARTBEAT_RESPONSE_V0 = new Schema(new Field("error_code", INT16));
    public static final Schema HEARTBEAT_RESPONSE_V1 = new Schema(
            newThrottleTimeField(),
            new Field("error_code", INT16));

    public static final Schema[] HEARTBEAT_REQUEST = {HEARTBEAT_REQUEST_V0, HEARTBEAT_REQUEST_V1};
    public static final Schema[] HEARTBEAT_RESPONSE = {HEARTBEAT_RESPONSE_V0, HEARTBEAT_RESPONSE_V1};

    /* Leave group api */
    public static final Schema LEAVE_GROUP_REQUEST_V0 = new Schema(new Field("group_id", STRING, "The group id."),
                                                                   new Field("member_id",
                                                                             STRING,
                                                                             "The member id assigned by the group coordinator."));

    /* v1 request is the same as v0. Throttle time has been added to response */
    public static final Schema LEAVE_GROUP_REQUEST_V1 = LEAVE_GROUP_REQUEST_V0;

    public static final Schema LEAVE_GROUP_RESPONSE_V0 = new Schema(new Field("error_code", INT16));
    public static final Schema LEAVE_GROUP_RESPONSE_V1 = new Schema(
            newThrottleTimeField(),
            new Field("error_code", INT16));

    public static final Schema[] LEAVE_GROUP_REQUEST = {LEAVE_GROUP_REQUEST_V0, LEAVE_GROUP_REQUEST_V1};
    public static final Schema[] LEAVE_GROUP_RESPONSE = {LEAVE_GROUP_RESPONSE_V0, LEAVE_GROUP_RESPONSE_V1};

    /* Leader and ISR api */
    public static final Schema LEADER_AND_ISR_REQUEST_PARTITION_STATE_V0 =
            new Schema(new Field("topic", STRING, "Topic name."),
                       new Field("partition", INT32, "Topic partition id."),
                       new Field("controller_epoch", INT32, "The controller epoch."),
                       new Field("leader", INT32, "The broker id for the leader."),
                       new Field("leader_epoch", INT32, "The leader epoch."),
                       new Field("isr", new ArrayOf(INT32), "The in sync replica ids."),
                       new Field("zk_version", INT32, "The ZK version."),
                       new Field("replicas", new ArrayOf(INT32), "The replica ids."));

    public static final Schema LEADER_AND_ISR_REQUEST_LIVE_LEADER_V0 =
            new Schema(new Field("id", INT32, "The broker id."),
                       new Field("host", STRING, "The hostname of the broker."),
                       new Field("port", INT32, "The port on which the broker accepts requests."));

    public static final Schema LEADER_AND_ISR_REQUEST_V0 = new Schema(new Field("controller_id", INT32, "The controller id."),
                                                                      new Field("controller_epoch", INT32, "The controller epoch."),
                                                                      new Field("partition_states",
                                                                                new ArrayOf(LEADER_AND_ISR_REQUEST_PARTITION_STATE_V0)),
                                                                      new Field("live_leaders", new ArrayOf(LEADER_AND_ISR_REQUEST_LIVE_LEADER_V0)));

    public static final Schema LEADER_AND_ISR_RESPONSE_PARTITION_V0 = new Schema(new Field("topic", STRING, "Topic name."),
                                                                                 new Field("partition", INT32, "Topic partition id."),
                                                                                 new Field("error_code", INT16, "Error code."));

    public static final Schema LEADER_AND_ISR_RESPONSE_V0 = new Schema(new Field("error_code", INT16, "Error code."),
                                                                       new Field("partitions",
                                                                                 new ArrayOf(LEADER_AND_ISR_RESPONSE_PARTITION_V0)));

    public static final Schema[] LEADER_AND_ISR_REQUEST = {LEADER_AND_ISR_REQUEST_V0};
    public static final Schema[] LEADER_AND_ISR_RESPONSE = {LEADER_AND_ISR_RESPONSE_V0};

    /* Replica api */
    public static final Schema STOP_REPLICA_REQUEST_PARTITION_V0 = new Schema(new Field("topic", STRING, "Topic name."),
                                                                              new Field("partition", INT32, "Topic partition id."));

    public static final Schema STOP_REPLICA_REQUEST_V0 = new Schema(new Field("controller_id", INT32, "The controller id."),
                                                                    new Field("controller_epoch", INT32, "The controller epoch."),
                                                                    new Field("delete_partitions", BOOLEAN,
                                                                              "Boolean which indicates if replica's partitions must be deleted."),
                                                                    new Field("partitions",
                                                                              new ArrayOf(STOP_REPLICA_REQUEST_PARTITION_V0)));

    public static final Schema STOP_REPLICA_RESPONSE_PARTITION_V0 = new Schema(new Field("topic", STRING, "Topic name."),
                                                                               new Field("partition", INT32, "Topic partition id."),
                                                                               new Field("error_code", INT16, "Error code."));

    public static final Schema STOP_REPLICA_RESPONSE_V0 = new Schema(new Field("error_code", INT16, "Error code."),
                                                                     new Field("partitions",
                                                                               new ArrayOf(STOP_REPLICA_RESPONSE_PARTITION_V0)));

    public static final Schema[] STOP_REPLICA_REQUEST = {STOP_REPLICA_REQUEST_V0};
    public static final Schema[] STOP_REPLICA_RESPONSE = {STOP_REPLICA_RESPONSE_V0};

    /* Update metadata api */

    public static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V0 = LEADER_AND_ISR_REQUEST_PARTITION_STATE_V0;

    public static final Schema UPDATE_METADATA_REQUEST_BROKER_V0 =
            new Schema(new Field("id", INT32, "The broker id."),
                       new Field("host", STRING, "The hostname of the broker."),
                       new Field("port", INT32, "The port on which the broker accepts requests."));

    public static final Schema UPDATE_METADATA_REQUEST_V0 = new Schema(new Field("controller_id", INT32, "The controller id."),
                                                                       new Field("controller_epoch", INT32, "The controller epoch."),
                                                                       new Field("partition_states",
                                                                                 new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V0)),
                                                                       new Field("live_brokers",
                                                                                 new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V0)));

    public static final Schema UPDATE_METADATA_RESPONSE_V0 = new Schema(new Field("error_code", INT16, "Error code."));

    public static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V1 = UPDATE_METADATA_REQUEST_PARTITION_STATE_V0;

    public static final Schema UPDATE_METADATA_REQUEST_END_POINT_V1 =
            // for some reason, V1 sends `port` before `host` while V0 sends `host` before `port
            new Schema(new Field("port", INT32, "The port on which the broker accepts requests."),
                       new Field("host", STRING, "The hostname of the broker."),
                       new Field("security_protocol_type", INT16, "The security protocol type."));

    public static final Schema UPDATE_METADATA_REQUEST_BROKER_V1 =
            new Schema(new Field("id", INT32, "The broker id."),
                       new Field("end_points", new ArrayOf(UPDATE_METADATA_REQUEST_END_POINT_V1)));

    public static final Schema UPDATE_METADATA_REQUEST_V1 = new Schema(new Field("controller_id", INT32, "The controller id."),
                                                                       new Field("controller_epoch", INT32, "The controller epoch."),
                                                                       new Field("partition_states",
                                                                                 new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V1)),
                                                                       new Field("live_brokers",
                                                                                 new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V1)));

    public static final Schema UPDATE_METADATA_RESPONSE_V1 = UPDATE_METADATA_RESPONSE_V0;

    public static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V2 = UPDATE_METADATA_REQUEST_PARTITION_STATE_V1;

    public static final Schema UPDATE_METADATA_REQUEST_END_POINT_V2 = UPDATE_METADATA_REQUEST_END_POINT_V1;

    public static final Schema UPDATE_METADATA_REQUEST_BROKER_V2 =
                    new Schema(new Field("id", INT32, "The broker id."),
                               new Field("end_points", new ArrayOf(UPDATE_METADATA_REQUEST_END_POINT_V2)),
                               new Field("rack", NULLABLE_STRING, "The rack"));

    public static final Schema UPDATE_METADATA_REQUEST_V2 =
            new Schema(new Field("controller_id", INT32, "The controller id."),
                       new Field("controller_epoch", INT32, "The controller epoch."),
                       new Field("partition_states", new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V2)),
                       new Field("live_brokers", new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V2)));

    public static final Schema UPDATE_METADATA_RESPONSE_V2 = UPDATE_METADATA_RESPONSE_V1;

    public static final Schema UPDATE_METADATA_REQUEST_PARTITION_STATE_V3 = UPDATE_METADATA_REQUEST_PARTITION_STATE_V2;

    public static final Schema UPDATE_METADATA_REQUEST_END_POINT_V3 =
            new Schema(new Field("port", INT32, "The port on which the broker accepts requests."),
                    new Field("host", STRING, "The hostname of the broker."),
                    new Field("listener_name", STRING, "The listener name."),
                    new Field("security_protocol_type", INT16, "The security protocol type."));

    public static final Schema UPDATE_METADATA_REQUEST_BROKER_V3 =
            new Schema(new Field("id", INT32, "The broker id."),
                    new Field("end_points", new ArrayOf(UPDATE_METADATA_REQUEST_END_POINT_V3)),
                    new Field("rack", NULLABLE_STRING, "The rack"));

    public static final Schema UPDATE_METADATA_REQUEST_V3 =
            new Schema(new Field("controller_id", INT32, "The controller id."),
                    new Field("controller_epoch", INT32, "The controller epoch."),
                    new Field("partition_states", new ArrayOf(UPDATE_METADATA_REQUEST_PARTITION_STATE_V3)),
                    new Field("live_brokers", new ArrayOf(UPDATE_METADATA_REQUEST_BROKER_V3)));

    public static final Schema UPDATE_METADATA_RESPONSE_V3 = UPDATE_METADATA_RESPONSE_V2;

    public static final Schema[] UPDATE_METADATA_REQUEST = {UPDATE_METADATA_REQUEST_V0, UPDATE_METADATA_REQUEST_V1,
        UPDATE_METADATA_REQUEST_V2, UPDATE_METADATA_REQUEST_V3};
    public static final Schema[] UPDATE_METADATA_RESPONSE = {UPDATE_METADATA_RESPONSE_V0, UPDATE_METADATA_RESPONSE_V1,
        UPDATE_METADATA_RESPONSE_V2, UPDATE_METADATA_RESPONSE_V3};

    /* SASL handshake api */
    public static final Schema SASL_HANDSHAKE_REQUEST_V0 = new Schema(
            new Field("mechanism", STRING, "SASL Mechanism chosen by the client."));

    public static final Schema SASL_HANDSHAKE_RESPONSE_V0 = new Schema(
            new Field("error_code", INT16),
            new Field("enabled_mechanisms", new ArrayOf(Type.STRING), "Array of mechanisms enabled in the server."));

    public static final Schema[] SASL_HANDSHAKE_REQUEST = {SASL_HANDSHAKE_REQUEST_V0};
    public static final Schema[] SASL_HANDSHAKE_RESPONSE = {SASL_HANDSHAKE_RESPONSE_V0};

    /* ApiVersion api */
    public static final Schema API_VERSIONS_REQUEST_V0 = new Schema();

    /* v1 request is the same as v0. Throttle time has been added to response */
    public static final Schema API_VERSIONS_REQUEST_V1 = API_VERSIONS_REQUEST_V0;

    public static final Schema API_VERSIONS_V0 = new Schema(new Field("api_key", INT16, "API key."),
                                                           new Field("min_version", INT16, "Minimum supported version."),
                                                           new Field("max_version", INT16, "Maximum supported version."));

    public static final Schema API_VERSIONS_RESPONSE_V0 = new Schema(new Field("error_code", INT16, "Error code."),
                                                                    new Field("api_versions", new ArrayOf(API_VERSIONS_V0), "API versions supported by the broker."));
    public static final Schema API_VERSIONS_RESPONSE_V1 = new Schema(
            new Field("error_code", INT16, "Error code."),
            new Field("api_versions", new ArrayOf(API_VERSIONS_V0), "API versions supported by the broker."),
            newThrottleTimeField());

    public static final Schema[] API_VERSIONS_REQUEST = new Schema[]{API_VERSIONS_REQUEST_V0, API_VERSIONS_REQUEST_V1};
    public static final Schema[] API_VERSIONS_RESPONSE = new Schema[]{API_VERSIONS_RESPONSE_V0, API_VERSIONS_RESPONSE_V1};

    /* Admin requests common */
    public static final Schema CONFIG_ENTRY = new Schema(new Field("config_name", STRING, "Configuration name"),
        new Field("config_value", NULLABLE_STRING, "Configuration value"));

    public static final Schema PARTITION_REPLICA_ASSIGNMENT_ENTRY = new Schema(
        new Field("partition_id", INT32),
        new Field("replicas", new ArrayOf(INT32), "The set of all nodes that should host this partition. The first replica in the list is the preferred leader."));

    public static final Schema TOPIC_ERROR_CODE = new Schema(new Field("topic", STRING), new Field("error_code", INT16));

    // Improves on TOPIC_ERROR_CODE by adding an error_message to complement the error_code
    public static final Schema TOPIC_ERROR = new Schema(new Field("topic", STRING), new Field("error_code", INT16),
            new Field("error_message", NULLABLE_STRING));

    /* CreateTopic api */
    public static final Schema SINGLE_CREATE_TOPIC_REQUEST_V0 = new Schema(
        new Field("topic",
            STRING,
            "Name for newly created topic."),
        new Field("num_partitions",
            INT32,
            "Number of partitions to be created. -1 indicates unset."),
        new Field("replication_factor",
            INT16,
            "Replication factor for the topic. -1 indicates unset."),
        new Field("replica_assignment",
            new ArrayOf(PARTITION_REPLICA_ASSIGNMENT_ENTRY),
            "Replica assignment among kafka brokers for this topic partitions. If this is set num_partitions and replication_factor must be unset."),
        new Field("config_entries",
            new ArrayOf(CONFIG_ENTRY),
            "Topic level configuration for topic to be set."));

    public static final Schema CREATE_TOPICS_REQUEST_V0 = new Schema(
        new Field("create_topic_requests",
            new ArrayOf(SINGLE_CREATE_TOPIC_REQUEST_V0),
            "An array of single topic creation requests. Can not have multiple entries for the same topic."),
        new Field("timeout",
            INT32,
            "The time in ms to wait for a topic to be completely created on the controller node. Values <= 0 will trigger topic creation and return immediately"));

    public static final Schema CREATE_TOPICS_RESPONSE_V0 = new Schema(
        new Field("topic_errors",
            new ArrayOf(TOPIC_ERROR_CODE),
            "An array of per topic error codes."));

    public static final Schema SINGLE_CREATE_TOPIC_REQUEST_V1 = SINGLE_CREATE_TOPIC_REQUEST_V0;

    public static final Schema CREATE_TOPICS_REQUEST_V1 = new Schema(
            new Field("create_topic_requests",
                    new ArrayOf(SINGLE_CREATE_TOPIC_REQUEST_V1),
                    "An array of single topic creation requests. Can not have multiple entries for the same topic."),
            new Field("timeout",
                    INT32,
                    "The time in ms to wait for a topic to be completely created on the controller node. Values <= 0 will trigger topic creation and return immediately"),
            new Field("validate_only",
                    BOOLEAN,
                    "If this is true, the request will be validated, but the topic won't be created."));

    public static final Schema CREATE_TOPICS_RESPONSE_V1 = new Schema(
            new Field("topic_errors",
                    new ArrayOf(TOPIC_ERROR),
                    "An array of per topic errors."));
    /* v2 request is the same as v1. Throttle time has been added to the response */
    public static final Schema CREATE_TOPICS_REQUEST_V2 = CREATE_TOPICS_REQUEST_V1;
    public static final Schema CREATE_TOPICS_RESPONSE_V2 = new Schema(
            newThrottleTimeField(),
            new Field("topic_errors",
                    new ArrayOf(TOPIC_ERROR),
                    "An array of per topic errors."));

    public static final Schema[] CREATE_TOPICS_REQUEST = {CREATE_TOPICS_REQUEST_V0, CREATE_TOPICS_REQUEST_V1, CREATE_TOPICS_REQUEST_V2};
    public static final Schema[] CREATE_TOPICS_RESPONSE = {CREATE_TOPICS_RESPONSE_V0, CREATE_TOPICS_RESPONSE_V1, CREATE_TOPICS_RESPONSE_V2};

    /* DeleteTopic api */
    public static final Schema DELETE_TOPICS_REQUEST_V0 = new Schema(
        new Field("topics",
            new ArrayOf(STRING),
            "An array of topics to be deleted."),
        new Field("timeout",
            INT32,
            "The time in ms to wait for a topic to be completely deleted on the controller node. Values <= 0 will trigger topic deletion and return immediately"));

    public static final Schema DELETE_TOPICS_RESPONSE_V0 = new Schema(
        new Field("topic_error_codes",
            new ArrayOf(TOPIC_ERROR_CODE),
            "An array of per topic error codes."));
    /* v1 request is the same as v0. Throttle time has been added to the response */
    public static final Schema DELETE_TOPICS_REQUEST_V1 = DELETE_TOPICS_REQUEST_V0;
    public static final Schema DELETE_TOPICS_RESPONSE_V1 = new Schema(
            newThrottleTimeField(),
            new Field("topic_error_codes",
                new ArrayOf(TOPIC_ERROR_CODE),
                "An array of per topic error codes."));

    public static final Schema[] DELETE_TOPICS_REQUEST = {DELETE_TOPICS_REQUEST_V0, DELETE_TOPICS_REQUEST_V1};
    public static final Schema[] DELETE_TOPICS_RESPONSE = {DELETE_TOPICS_RESPONSE_V0, DELETE_TOPICS_RESPONSE_V1};

    public static final Schema DELETE_RECORDS_REQUEST_PARTITION_V0 = new Schema(new Field("partition", INT32, "Topic partition id."),
                                                                                new Field("offset", INT64, "The offset before which the messages will be deleted."));

    public static final Schema DELETE_RECORDS_REQUEST_TOPIC_V0 = new Schema(new Field("topic", STRING, "Topic name."),
                                                                            new Field("partitions", new ArrayOf(DELETE_RECORDS_REQUEST_PARTITION_V0)));

    public static final Schema DELETE_RECORDS_REQUEST_V0 = new Schema(new Field("topics", new ArrayOf(DELETE_RECORDS_REQUEST_TOPIC_V0)),
                                                                      new Field("timeout", INT32, "The maximum time to await a response in ms."));

    public static final Schema DELETE_RECORDS_RESPONSE_PARTITION_V0 = new Schema(new Field("partition", INT32, "Topic partition id."),
                                                                                 new Field("low_watermark", INT64, "Smallest available offset of all live replicas"),
                                                                                 new Field("error_code", INT16, "The error code for the given partition."));

    public static final Schema DELETE_RECORDS_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING, "Topic name."),
                                                                             new Field("partitions", new ArrayOf(DELETE_RECORDS_RESPONSE_PARTITION_V0)));

    public static final Schema DELETE_RECORDS_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("topics", new ArrayOf(DELETE_RECORDS_RESPONSE_TOPIC_V0)));

    public static final Schema[] DELETE_RECORDS_REQUEST = {DELETE_RECORDS_REQUEST_V0};
    public static final Schema[] DELETE_RECORDS_RESPONSE = {DELETE_RECORDS_RESPONSE_V0};

    /* Transactions API */
    public static final Schema INIT_PRODUCER_ID_REQUEST_V0 = new Schema(
            new Field("transactional_id",
                    NULLABLE_STRING,
                    "The transactional id whose producer id we want to retrieve or generate."),
            new Field("transaction_timeout_ms",
                    INT32,
                    "The time in ms to wait for before aborting idle transactions sent by this producer.")
    );

    public static final Schema INIT_PRODUCER_ID_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("error_code",
                    INT16,
                    "An integer error code."),
            new Field("producer_id",
                    INT64,
                    "The producer id for the input transactional id. If the input id was empty, then this is used only for ensuring idempotence of messages."),
            new Field("producer_epoch",
                    INT16,
                    "The epoch for the producer id. Will always be 0 if no transactional id was specified in the request.")
    );

    public static final Schema[] INIT_PRODUCER_ID_REQUEST = {INIT_PRODUCER_ID_REQUEST_V0};

    public static final Schema[] INIT_PRODUCER_ID_RESPONSE = {INIT_PRODUCER_ID_RESPONSE_V0};

    /* Offsets for Leader Epoch api */
    public static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0 = new Schema(
            new Field("partition_id",
                    INT32,
                    "The partition_id"),
            new Field("leader_epoch",
                    INT32,
                    "The epoch")
    );
    public static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V0 = new Schema(
            new Field("topic",
                    STRING,
                    "The topic"),
            new Field("partitions",
                    new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0),
                    "The partition")
            );
    public static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V0 = new Schema(
            new Field("topics",
                    new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V0),
                    "An array of topics to get epochs for"));


    public static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_PARTITION_V0 = new Schema(
            new Field("error_code",
                    INT16,
                    "The error code"),
            new Field("partition_id",
                    INT32,
                    "The partition id"),
            new Field("end_offset",
                    INT64,
                    "The end offset")
    );
    public static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_TOPIC_V0 = new Schema(
            new Field("topic",
                     STRING,
                    "The topic"),
            new Field("partitions",
                    new ArrayOf(OFFSET_FOR_LEADER_EPOCH_RESPONSE_PARTITION_V0),
                    "The partition")
            );
    public static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_V0 = new Schema(
            new Field("topics",
                    new ArrayOf(OFFSET_FOR_LEADER_EPOCH_RESPONSE_TOPIC_V0),
                    "An array of topics for which we have leader offsets for some requested Partition Leader Epoch"));

    public static final Schema[] OFFSET_FOR_LEADER_EPOCH_REQUEST = {OFFSET_FOR_LEADER_EPOCH_REQUEST_V0};
    public static final Schema[] OFFSET_FOR_LEADER_EPOCH_RESPONSE = {OFFSET_FOR_LEADER_EPOCH_RESPONSE_V0};

    public static final Schema ADD_PARTITIONS_TO_TXN_REQUEST_V0 = new Schema(
            new Field("transactional_id",
                    STRING,
                    "The transactional id corresponding to the transaction."),
            new Field("producer_id",
                    INT64,
                    "Current producer id in use by the transactional id."),
            new Field("producer_epoch",
                    INT16,
                    "Current epoch associated with the producer id."),
            new Field("topics",
                    new ArrayOf(new Schema(
                            new Field("topic", STRING),
                            new Field("partitions", new ArrayOf(INT32)))),
                    "The partitions to add to the transaction.")
    );
    public static final Schema ADD_PARTITIONS_TO_TXN_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("errors",
                      new ArrayOf(new Schema(new Field("topic", STRING),
                                   new Field("partition_errors",
                                             new ArrayOf(new Schema(new Field("partition",
                                                                              INT32),
                                                                    new Field("error_code",
                                                                              INT16)))))))
    );

    public static final Schema[] ADD_PARTITIONS_TO_TXN_REQUEST = {ADD_PARTITIONS_TO_TXN_REQUEST_V0};
    public static final Schema[] ADD_PARTITIONS_TO_TXN_RESPONSE = {ADD_PARTITIONS_TO_TXN_RESPONSE_V0};

    public static final Schema ADD_OFFSETS_TO_TXN_REQUEST_V0 = new Schema(
            new Field("transactional_id",
                    STRING,
                    "The transactional id corresponding to the transaction."),
            new Field("producer_id",
                    INT64,
                    "Current producer id in use by the transactional id."),
            new Field("producer_epoch",
                    INT16,
                    "Current epoch associated with the producer id."),
            new Field("consumer_group_id",
                    STRING,
                    "Consumer group id whose offsets should be included in the transaction.")
    );
    public static final Schema ADD_OFFSETS_TO_TXN_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("error_code",
                    INT16,
                    "An integer error code.")
    );

    public static final Schema[] ADD_OFFSETS_TO_TXN_REQUEST = {ADD_OFFSETS_TO_TXN_REQUEST_V0};
    public static final Schema[] ADD_OFFSETS_TO_TXN_RESPONSE = {ADD_OFFSETS_TO_TXN_RESPONSE_V0};

    public static final Schema END_TXN_REQUEST_V0 = new Schema(
            new Field("transactional_id",
                    STRING,
                    "The transactional id corresponding to the transaction."),
            new Field("producer_id",
                    INT64,
                    "Current producer id in use by the transactional id."),
            new Field("producer_epoch",
                    INT16,
                    "Current epoch associated with the producer id."),
            new Field("transaction_result",
                    BOOLEAN,
                    "The result of the transaction (0 = ABORT, 1 = COMMIT)")
    );

    public static final Schema END_TXN_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("error_code",
                    INT16,
                    "An integer error code.")
    );

    public static final Schema[] END_TXN_REQUEST = {END_TXN_REQUEST_V0};
    public static final Schema[] END_TXN_RESPONSE = {END_TXN_RESPONSE_V0};

    public static final Schema WRITE_TXN_MARKERS_ENTRY_V0 = new Schema(
            new Field("producer_id",
                    INT64,
                    "Current producer id in use by the transactional id."),
            new Field("producer_epoch",
                    INT16,
                    "Current epoch associated with the producer id."),
            new Field("transaction_result",
                    BOOLEAN,
                    "The result of the transaction to write to the partitions (false = ABORT, true = COMMIT)."),
            new Field("topics",
                    new ArrayOf(new Schema(
                            new Field("topic", STRING),
                            new Field("partitions", new ArrayOf(INT32)))),
                    "The partitions to write markers for."),
            new Field("coordinator_epoch",
                      INT32,
                      "Epoch associated with the transaction state partition hosted by this transaction coordinator")
    );

    public static final Schema WRITE_TXN_MARKERS_REQUEST_V0 = new Schema(
            new Field("transaction_markers",
                    new ArrayOf(WRITE_TXN_MARKERS_ENTRY_V0),
                    "The transaction markers to be written.")
    );

    public static final Schema WRITE_TXN_MARKERS_PARTITION_ERROR_RESPONSE_V0 = new Schema(
            new Field("partition", INT32),
            new Field("error_code", INT16)
    );

    public static final Schema WRITE_TXN_MARKERS_ENTRY_RESPONSE_V0 = new Schema(
            new Field("producer_id",
                    INT64,
                    "Current producer id in use by the transactional id."),
            new Field("topics",
                    new ArrayOf(new Schema(
                            new Field("topic", STRING),
                            new Field("partitions", new ArrayOf(WRITE_TXN_MARKERS_PARTITION_ERROR_RESPONSE_V0)))),
                    "Errors per partition from writing markers.")
    );

    public static final Schema WRITE_TXN_MARKERS_RESPONSE_V0 = new Schema(
            new Field("transaction_markers", new ArrayOf(WRITE_TXN_MARKERS_ENTRY_RESPONSE_V0), "Errors per partition from writing markers.")
    );

    public static final Schema[] WRITE_TXN_REQUEST = {WRITE_TXN_MARKERS_REQUEST_V0};
    public static final Schema[] WRITE_TXN_RESPONSE = {WRITE_TXN_MARKERS_RESPONSE_V0};

    public static final Schema TXN_OFFSET_COMMIT_PARTITION_OFFSET_METADATA_REQUEST_V0 = new Schema(
            new Field("partition", INT32),
            new Field("offset", INT64),
            new Field("metadata", NULLABLE_STRING)
    );

    public static final Schema TXN_OFFSET_COMMIT_REQUEST_V0 = new Schema(
            new Field("transactional_id",
                    STRING,
                    "The transactional id corresponding to the transaction."),
            new Field("consumer_group_id",
                    STRING,
                    "Id of the associated consumer group to commit offsets for."),
            new Field("producer_id",
                    INT64,
                    "Current producer id in use by the transactional id."),
            new Field("producer_epoch",
                    INT16,
                    "Current epoch associated with the producer id."),
            new Field("topics",
                    new ArrayOf(new Schema(
                            new Field("topic", STRING),
                            new Field("partitions", new ArrayOf(TXN_OFFSET_COMMIT_PARTITION_OFFSET_METADATA_REQUEST_V0)))),
                    "The partitions to write markers for.")
    );

    public static final Schema TXN_OFFSET_COMMIT_PARTITION_ERROR_RESPONSE_V0 = new Schema(
            new Field("partition", INT32),
            new Field("error_code", INT16)
    );

    public static final Schema TXN_OFFSET_COMMIT_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("topics",
                    new ArrayOf(new Schema(
                            new Field("topic", STRING),
                            new Field("partitions", new ArrayOf(TXN_OFFSET_COMMIT_PARTITION_ERROR_RESPONSE_V0)))),
                    "Errors per partition from writing markers.")
    );

    public static final Schema[] TXN_OFFSET_COMMIT_REQUEST = {TXN_OFFSET_COMMIT_REQUEST_V0};
    public static final Schema[] TXN_OFFSET_COMMIT_RESPONSE = {TXN_OFFSET_COMMIT_RESPONSE_V0};

    /* DescribeConfigs API */

    public static final Schema DESCRIBE_CONFIGS_REQUEST_RESOURCE_V0 = new Schema(
            new Field("resource_type", INT8),
            new Field("resource_name", STRING),
            new Field("config_names", ArrayOf.nullable(STRING))
    );

    public static final Schema DESCRIBE_CONFIGS_REQUEST_V0 = new Schema(
            new Field("resources", new ArrayOf(DESCRIBE_CONFIGS_REQUEST_RESOURCE_V0),
                    "An array of config resources to be returned."));

    public static final Schema DESCRIBE_CONFIGS_RESPONSE_ENTITY_V0 = new Schema(
            new Field("error_code", INT16),
            new Field("error_message", NULLABLE_STRING),
            new Field("resource_type", INT8),
            new Field("resource_name", STRING),
            new Field("config_entries", new ArrayOf(new Schema(
                    new Field("config_name", STRING),
                    new Field("config_value", NULLABLE_STRING),
                    new Field("read_only", BOOLEAN),
                    new Field("is_default", BOOLEAN),
                    new Field("is_sensitive", BOOLEAN)
            ))
    ));

    public static final Schema DESCRIBE_CONFIGS_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("resources", new ArrayOf(DESCRIBE_CONFIGS_RESPONSE_ENTITY_V0)));

    public static final Schema[] DESCRIBE_CONFIGS_REQUEST = {DESCRIBE_CONFIGS_REQUEST_V0};
    public static final Schema[] DESCRIBE_CONFIGS_RESPONSE = {DESCRIBE_CONFIGS_RESPONSE_V0};

    /* AlterConfigs API */

    public static final Schema ALTER_CONFIGS_REQUEST_RESOURCE_V0 = new Schema(
            new Field("resource_type", INT8),
            new Field("resource_name", STRING),
            new Field("config_entries", new ArrayOf(CONFIG_ENTRY)));

    public static final Schema ALTER_CONFIGS_REQUEST_V0 = new Schema(
            new Field("resources", new ArrayOf(ALTER_CONFIGS_REQUEST_RESOURCE_V0),
                    "An array of resources to update with the provided configs."),
            new Field("validate_only", BOOLEAN));

    public static final Schema ALTER_CONFIGS_RESPONSE_ENTITY_V0 = new Schema(
            new Field("error_code", INT16),
            new Field("error_message", NULLABLE_STRING),
            new Field("resource_type", INT8),
            new Field("resource_name", STRING));

    public static final Schema ALTER_CONFIGS_RESPONSE_V0 = new Schema(
            newThrottleTimeField(),
            new Field("resources", new ArrayOf(ALTER_CONFIGS_RESPONSE_ENTITY_V0)));

    public static final Schema[] ALTER_CONFIGS_REQUEST = {ALTER_CONFIGS_REQUEST_V0};
    public static final Schema[] ALTER_CONFIGS_RESPONSE = {ALTER_CONFIGS_RESPONSE_V0};

    public static final Schema DESCRIBE_ACLS_REQUEST_V0 = new Schema(
        new Field("resource_type", INT8, "The filter resource type."),
        new Field("resource_name", NULLABLE_STRING, "The filter resource name."),
        new Field("principal", NULLABLE_STRING, "The filter principal name."),
        new Field("host", NULLABLE_STRING, "The filter ip address."),
        new Field("operation", INT8, "The filter operation type."),
        new Field("permission_type", INT8, "The filter permission type.")
    );

    public static final Schema DESCRIBE_ACLS_RESOURCE = new Schema(
        new Field("resource_type", INT8, "The resource type"),
        new Field("resource_name", STRING, "The resource name"),
        new Field("acls", new ArrayOf(new Schema(
            new Field("principal", STRING, "The ACL principal"),
            new Field("host", STRING, "The ACL host"),
            new Field("operation", INT8, "The ACL operation"),
            new Field("permission_type", INT8, "The ACL permission type")))));

    public static final Schema DESCRIBE_ACLS_RESPONSE_V0 = new Schema(
        newThrottleTimeField(),
        new Field("error_code", INT16, "The error code."),
        new Field("error_message", NULLABLE_STRING, "The error message."),
        new Field("resources",
            new ArrayOf(DESCRIBE_ACLS_RESOURCE),
            "The resources and their associated ACLs."));

    public static final Schema[] DESCRIBE_ACLS_REQUEST = new Schema[] {DESCRIBE_ACLS_REQUEST_V0};
    public static final Schema[] DESCRIBE_ACLS_RESPONSE  = new Schema[] {DESCRIBE_ACLS_RESPONSE_V0};

    public static final Schema CREATE_ACLS_REQUEST_V0 = new Schema(
        new Field("creations",
            new ArrayOf(new Schema(
                new Field("resource_type", INT8, "The resource type."),
                new Field("resource_name", STRING, "The resource name."),
                new Field("principal", STRING, "The principal."),
                new Field("host", STRING, "The ip address."),
                new Field("operation", INT8, "The ACL operation"),
                new Field("permission_type", INT8, "The ACL permission type")
            ))));

    public static final Schema CREATE_ACLS_RESPONSE_V0 = new Schema(
        newThrottleTimeField(),
        new Field("creation_responses",
            new ArrayOf(new Schema(
                new Field("error_code", INT16, "The error code."),
                new Field("error_message", NULLABLE_STRING, "The error message.")
            ))));

    public static final Schema[] CREATE_ACLS_REQUEST = new Schema[] {CREATE_ACLS_REQUEST_V0};
    public static final Schema[] CREATE_ACLS_RESPONSE = new Schema[] {CREATE_ACLS_RESPONSE_V0};

    public static final Schema DELETE_ACLS_REQUEST_V0 = new Schema(
        new Field("filters",
            new ArrayOf(new Schema(
                new Field("resource_type", INT8, "The resource type filter."),
                new Field("resource_name", NULLABLE_STRING, "The resource name filter."),
                new Field("principal", NULLABLE_STRING, "The principal filter."),
                new Field("host", NULLABLE_STRING, "The ip address filter."),
                new Field("operation", INT8, "The ACL operation filter."),
                new Field("permission_type", INT8, "The ACL permission type filter.")
            ))));

    public static final Schema MATCHING_ACL = new Schema(
        new Field("error_code", INT16, "The error code."),
        new Field("error_message", NULLABLE_STRING, "The error message."),
        new Field("resource_type", INT8, "The resource type."),
        new Field("resource_name", STRING, "The resource name."),
        new Field("principal", STRING, "The principal."),
        new Field("host", STRING, "The ip address."),
        new Field("operation", INT8, "The ACL operation"),
        new Field("permission_type", INT8, "The ACL permission type")
    );

    public static final Schema DELETE_ACLS_RESPONSE_V0 = new Schema(
        newThrottleTimeField(),
        new Field("filter_responses",
            new ArrayOf(new Schema(
                new Field("error_code", INT16, "The error code."),
                new Field("error_message", NULLABLE_STRING, "The error message."),
                new Field("matching_acls", new ArrayOf(MATCHING_ACL), "The matching ACLs")))));

    public static final Schema[] DELETE_ACLS_REQUEST = new Schema[] {DELETE_ACLS_REQUEST_V0};
    public static final Schema[] DELETE_ACLS_RESPONSE = new Schema[] {DELETE_ACLS_RESPONSE_V0};

    /* an array of all requests and responses with all schema versions; a null value in the inner array means that the
     * particular version is not supported */
    public static final Schema[][] REQUESTS = new Schema[ApiKeys.MAX_API_KEY + 1][];
    public static final Schema[][] RESPONSES = new Schema[ApiKeys.MAX_API_KEY + 1][];
    static final short[] MIN_VERSIONS = new short[ApiKeys.MAX_API_KEY + 1];

    /* the latest version of each api */
    static final short[] CURR_VERSION = new short[ApiKeys.MAX_API_KEY + 1];

    static {
        REQUESTS[ApiKeys.PRODUCE.id] = PRODUCE_REQUEST;
        REQUESTS[ApiKeys.FETCH.id] = FETCH_REQUEST;
        REQUESTS[ApiKeys.LIST_OFFSETS.id] = LIST_OFFSET_REQUEST;
        REQUESTS[ApiKeys.METADATA.id] = METADATA_REQUEST;
        REQUESTS[ApiKeys.LEADER_AND_ISR.id] = LEADER_AND_ISR_REQUEST;
        REQUESTS[ApiKeys.STOP_REPLICA.id] = STOP_REPLICA_REQUEST;
        REQUESTS[ApiKeys.UPDATE_METADATA_KEY.id] = UPDATE_METADATA_REQUEST;
        REQUESTS[ApiKeys.CONTROLLED_SHUTDOWN_KEY.id] = CONTROLLED_SHUTDOWN_REQUEST;
        REQUESTS[ApiKeys.OFFSET_COMMIT.id] = OFFSET_COMMIT_REQUEST;
        REQUESTS[ApiKeys.OFFSET_FETCH.id] = OFFSET_FETCH_REQUEST;
        REQUESTS[ApiKeys.FIND_COORDINATOR.id] = FIND_COORDINATOR_REQUEST;
        REQUESTS[ApiKeys.JOIN_GROUP.id] = JOIN_GROUP_REQUEST;
        REQUESTS[ApiKeys.HEARTBEAT.id] = HEARTBEAT_REQUEST;
        REQUESTS[ApiKeys.LEAVE_GROUP.id] = LEAVE_GROUP_REQUEST;
        REQUESTS[ApiKeys.SYNC_GROUP.id] = SYNC_GROUP_REQUEST;
        REQUESTS[ApiKeys.DESCRIBE_GROUPS.id] = DESCRIBE_GROUPS_REQUEST;
        REQUESTS[ApiKeys.LIST_GROUPS.id] = LIST_GROUPS_REQUEST;
        REQUESTS[ApiKeys.SASL_HANDSHAKE.id] = SASL_HANDSHAKE_REQUEST;
        REQUESTS[ApiKeys.API_VERSIONS.id] = API_VERSIONS_REQUEST;
        REQUESTS[ApiKeys.CREATE_TOPICS.id] = CREATE_TOPICS_REQUEST;
        REQUESTS[ApiKeys.DELETE_TOPICS.id] = DELETE_TOPICS_REQUEST;
        REQUESTS[ApiKeys.DELETE_RECORDS.id] = DELETE_RECORDS_REQUEST;
        REQUESTS[ApiKeys.INIT_PRODUCER_ID.id] = INIT_PRODUCER_ID_REQUEST;
        REQUESTS[ApiKeys.OFFSET_FOR_LEADER_EPOCH.id] = OFFSET_FOR_LEADER_EPOCH_REQUEST;
        REQUESTS[ApiKeys.ADD_PARTITIONS_TO_TXN.id] = ADD_PARTITIONS_TO_TXN_REQUEST;
        REQUESTS[ApiKeys.ADD_OFFSETS_TO_TXN.id] = ADD_OFFSETS_TO_TXN_REQUEST;
        REQUESTS[ApiKeys.END_TXN.id] = END_TXN_REQUEST;
        REQUESTS[ApiKeys.WRITE_TXN_MARKERS.id] = WRITE_TXN_REQUEST;
        REQUESTS[ApiKeys.TXN_OFFSET_COMMIT.id] = TXN_OFFSET_COMMIT_REQUEST;
        REQUESTS[ApiKeys.DESCRIBE_ACLS.id] = DESCRIBE_ACLS_REQUEST;
        REQUESTS[ApiKeys.CREATE_ACLS.id] = CREATE_ACLS_REQUEST;
        REQUESTS[ApiKeys.DELETE_ACLS.id] = DELETE_ACLS_REQUEST;
        REQUESTS[ApiKeys.DESCRIBE_CONFIGS.id] = DESCRIBE_CONFIGS_REQUEST;
        REQUESTS[ApiKeys.ALTER_CONFIGS.id] = ALTER_CONFIGS_REQUEST;

        RESPONSES[ApiKeys.PRODUCE.id] = PRODUCE_RESPONSE;
        RESPONSES[ApiKeys.FETCH.id] = FETCH_RESPONSE;
        RESPONSES[ApiKeys.LIST_OFFSETS.id] = LIST_OFFSET_RESPONSE;
        RESPONSES[ApiKeys.METADATA.id] = METADATA_RESPONSE;
        RESPONSES[ApiKeys.LEADER_AND_ISR.id] = LEADER_AND_ISR_RESPONSE;
        RESPONSES[ApiKeys.STOP_REPLICA.id] = STOP_REPLICA_RESPONSE;
        RESPONSES[ApiKeys.UPDATE_METADATA_KEY.id] = UPDATE_METADATA_RESPONSE;
        RESPONSES[ApiKeys.CONTROLLED_SHUTDOWN_KEY.id] = CONTROLLED_SHUTDOWN_RESPONSE;
        RESPONSES[ApiKeys.OFFSET_COMMIT.id] = OFFSET_COMMIT_RESPONSE;
        RESPONSES[ApiKeys.OFFSET_FETCH.id] = OFFSET_FETCH_RESPONSE;
        RESPONSES[ApiKeys.FIND_COORDINATOR.id] = FIND_COORDINATOR_RESPONSE;
        RESPONSES[ApiKeys.JOIN_GROUP.id] = JOIN_GROUP_RESPONSE;
        RESPONSES[ApiKeys.HEARTBEAT.id] = HEARTBEAT_RESPONSE;
        RESPONSES[ApiKeys.LEAVE_GROUP.id] = LEAVE_GROUP_RESPONSE;
        RESPONSES[ApiKeys.SYNC_GROUP.id] = SYNC_GROUP_RESPONSE;
        RESPONSES[ApiKeys.DESCRIBE_GROUPS.id] = DESCRIBE_GROUPS_RESPONSE;
        RESPONSES[ApiKeys.LIST_GROUPS.id] = LIST_GROUPS_RESPONSE;
        RESPONSES[ApiKeys.SASL_HANDSHAKE.id] = SASL_HANDSHAKE_RESPONSE;
        RESPONSES[ApiKeys.API_VERSIONS.id] = API_VERSIONS_RESPONSE;
        RESPONSES[ApiKeys.CREATE_TOPICS.id] = CREATE_TOPICS_RESPONSE;
        RESPONSES[ApiKeys.DELETE_TOPICS.id] = DELETE_TOPICS_RESPONSE;
        RESPONSES[ApiKeys.DELETE_RECORDS.id] = DELETE_RECORDS_RESPONSE;
        RESPONSES[ApiKeys.INIT_PRODUCER_ID.id] = INIT_PRODUCER_ID_RESPONSE;
        RESPONSES[ApiKeys.OFFSET_FOR_LEADER_EPOCH.id] = OFFSET_FOR_LEADER_EPOCH_RESPONSE;
        RESPONSES[ApiKeys.ADD_PARTITIONS_TO_TXN.id] = ADD_PARTITIONS_TO_TXN_RESPONSE;
        RESPONSES[ApiKeys.ADD_OFFSETS_TO_TXN.id] = ADD_OFFSETS_TO_TXN_RESPONSE;
        RESPONSES[ApiKeys.END_TXN.id] = END_TXN_RESPONSE;
        RESPONSES[ApiKeys.WRITE_TXN_MARKERS.id] = WRITE_TXN_RESPONSE;
        RESPONSES[ApiKeys.TXN_OFFSET_COMMIT.id] = TXN_OFFSET_COMMIT_RESPONSE;
        RESPONSES[ApiKeys.DESCRIBE_ACLS.id] = DESCRIBE_ACLS_RESPONSE;
        RESPONSES[ApiKeys.CREATE_ACLS.id] = CREATE_ACLS_RESPONSE;
        RESPONSES[ApiKeys.DELETE_ACLS.id] = DELETE_ACLS_RESPONSE;
        RESPONSES[ApiKeys.DESCRIBE_CONFIGS.id] = DESCRIBE_CONFIGS_RESPONSE;
        RESPONSES[ApiKeys.ALTER_CONFIGS.id] = ALTER_CONFIGS_RESPONSE;

        /* set the minimum and maximum version of each api */
        for (ApiKeys api : ApiKeys.values()) {
            CURR_VERSION[api.id] = (short) (REQUESTS[api.id].length - 1);
            for (int i = 0; i < REQUESTS[api.id].length; ++i)
                if (REQUESTS[api.id][i] != null) {
                    MIN_VERSIONS[api.id] = (short) i;
                    break;
                }
        }

        /* sanity check that:
         *   - we have the same number of request and response versions for each api
         *   - we have a consistent set of request and response versions for each api */
        for (ApiKeys api : ApiKeys.values()) {
            if (REQUESTS[api.id].length != RESPONSES[api.id].length)
                throw new IllegalStateException(REQUESTS[api.id].length + " request versions for api " + api.name
                        + " but " + RESPONSES[api.id].length + " response versions.");

            for (int i = 0; i < REQUESTS[api.id].length; ++i)
                if ((REQUESTS[api.id][i] == null && RESPONSES[api.id][i] != null) ||
                        (REQUESTS[api.id][i] != null && RESPONSES[api.id][i] == null))
                    throw new IllegalStateException("Request and response for version " + i + " of API "
                            + api.id + " are defined inconsistently. One is null while the other is not null.");
        }
    }

    public static boolean apiVersionSupported(short apiKey, short apiVersion) {
        return apiKey < CURR_VERSION.length && apiVersion >= MIN_VERSIONS[apiKey] && apiVersion <= CURR_VERSION[apiKey];
    }

    private static Field newThrottleTimeField() {
        return new Field("throttle_time_ms", INT32,
                "Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not violate any quota.)",
                0);
    }

    private static String indentString(int size) {
        StringBuilder b = new StringBuilder(size);
        for (int i = 0; i < size; i++)
            b.append(" ");
        return b.toString();
    }

    private static void schemaToBnfHtml(Schema schema, StringBuilder b, int indentSize) {
        final String indentStr = indentString(indentSize);
        final Map<String, Type> subTypes = new LinkedHashMap<>();

        // Top level fields
        for (Field field: schema.fields()) {
            if (field.type instanceof ArrayOf) {
                b.append("[");
                b.append(field.name);
                b.append("] ");
                Type innerType = ((ArrayOf) field.type).type();
                if (!subTypes.containsKey(field.name))
                    subTypes.put(field.name, innerType);
            } else {
                b.append(field.name);
                b.append(" ");
                if (!subTypes.containsKey(field.name))
                    subTypes.put(field.name, field.type);
            }
        }
        b.append("\n");

        // Sub Types/Schemas
        for (Map.Entry<String, Type> entry: subTypes.entrySet()) {
            if (entry.getValue() instanceof Schema) {
                // Complex Schema Type
                b.append(indentStr);
                b.append(entry.getKey());
                b.append(" => ");
                schemaToBnfHtml((Schema) entry.getValue(), b, indentSize + 2);
            } else {
                // Standard Field Type
                b.append(indentStr);
                b.append(entry.getKey());
                b.append(" => ");
                b.append(entry.getValue());
                b.append("\n");
            }
        }
    }

    private static void populateSchemaFields(Schema schema, Set<Field> fields) {
        for (Field field: schema.fields()) {
            fields.add(field);
            if (field.type instanceof ArrayOf) {
                Type innerType = ((ArrayOf) field.type).type();
                if (innerType instanceof Schema)
                    populateSchemaFields((Schema) innerType, fields);
            } else if (field.type instanceof Schema)
                populateSchemaFields((Schema) field.type, fields);
        }
    }

    private static void schemaToFieldTableHtml(Schema schema, StringBuilder b) {
        Set<Field> fields = new LinkedHashSet<>();
        populateSchemaFields(schema, fields);

        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Field</th>\n");
        b.append("<th>Description</th>\n");
        b.append("</tr>");
        for (Field field : fields) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append(field.name);
            b.append("</td>");
            b.append("<td>");
            b.append(field.doc);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
    }

    public static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<h5>Headers:</h5>\n");

        b.append("<pre>");
        b.append("Request Header => ");
        schemaToBnfHtml(REQUEST_HEADER, b, 2);
        b.append("</pre>\n");
        schemaToFieldTableHtml(REQUEST_HEADER, b);

        b.append("<pre>");
        b.append("Response Header => ");
        schemaToBnfHtml(RESPONSE_HEADER, b, 2);
        b.append("</pre>\n");
        schemaToFieldTableHtml(RESPONSE_HEADER, b);

        for (ApiKeys key : ApiKeys.values()) {
            // Key
            b.append("<h5>");
            b.append("<a name=\"The_Messages_" + key.name + "\">");
            b.append(key.name);
            b.append(" API (Key: ");
            b.append(key.id);
            b.append("):</a></h5>\n\n");
            // Requests
            b.append("<b>Requests:</b><br>\n");
            Schema[] requests = REQUESTS[key.id];
            for (int i = 0; i < requests.length; i++) {
                Schema schema = requests[i];
                // Schema
                if (schema != null) {
                    b.append("<p>");
                    // Version header
                    b.append("<pre>");
                    b.append(key.name);
                    b.append(" Request (Version: ");
                    b.append(i);
                    b.append(") => ");
                    schemaToBnfHtml(requests[i], b, 2);
                    b.append("</pre>");
                    schemaToFieldTableHtml(requests[i], b);
                }
                b.append("</p>\n");
            }

            // Responses
            b.append("<b>Responses:</b><br>\n");
            Schema[] responses = RESPONSES[key.id];
            for (int i = 0; i < responses.length; i++) {
                Schema schema = responses[i];
                // Schema
                if (schema != null) {
                    b.append("<p>");
                    // Version header
                    b.append("<pre>");
                    b.append(key.name);
                    b.append(" Response (Version: ");
                    b.append(i);
                    b.append(") => ");
                    schemaToBnfHtml(responses[i], b, 2);
                    b.append("</pre>");
                    schemaToFieldTableHtml(responses[i], b);
                }
                b.append("</p>\n");
            }
        }

        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

}
