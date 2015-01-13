/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.STRING;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;

public class Protocol {

    public static Schema REQUEST_HEADER = new Schema(new Field("api_key", INT16, "The id of the request type."),
                                                     new Field("api_version", INT16, "The version of the API."),
                                                     new Field("correlation_id",
                                                               INT32,
                                                               "A user-supplied integer value that will be passed back with the response"),
                                                     new Field("client_id",
                                                               STRING,
                                                               "A user specified identifier for the client making the request."));

    public static Schema RESPONSE_HEADER = new Schema(new Field("correlation_id",
                                                                INT32,
                                                                "The user-supplied value passed in with the request"));

    /* Metadata api */

    public static Schema METADATA_REQUEST_V0 = new Schema(new Field("topics",
                                                                    new ArrayOf(STRING),
                                                                    "An array of topics to fetch metadata for. If no topics are specified fetch metadtata for all topics."));

    public static Schema BROKER = new Schema(new Field("node_id", INT32, "The broker id."),
                                             new Field("host", STRING, "The hostname of the broker."),
                                             new Field("port", INT32, "The port on which the broker accepts requests."));

    public static Schema PARTITION_METADATA_V0 = new Schema(new Field("partition_error_code",
                                                                      INT16,
                                                                      "The error code for the partition, if any."),
                                                            new Field("partition_id", INT32, "The id of the partition."),
                                                            new Field("leader",
                                                                      INT32,
                                                                      "The id of the broker acting as leader for this partition."),
                                                            new Field("replicas",
                                                                      new ArrayOf(INT32),
                                                                      "The set of all nodes that host this partition."),
                                                            new Field("isr",
                                                                      new ArrayOf(INT32),
                                                                      "The set of nodes that are in sync with the leader for this partition."));

    public static Schema TOPIC_METADATA_V0 = new Schema(new Field("topic_error_code", INT16, "The error code for the given topic."),
                                                        new Field("topic", STRING, "The name of the topic"),
                                                        new Field("partition_metadata",
                                                                  new ArrayOf(PARTITION_METADATA_V0),
                                                                  "Metadata for each partition of the topic."));

    public static Schema METADATA_RESPONSE_V0 = new Schema(new Field("brokers",
                                                                     new ArrayOf(BROKER),
                                                                     "Host and port information for all brokers."),
                                                           new Field("topic_metadata", new ArrayOf(TOPIC_METADATA_V0)));

    public static Schema[] METADATA_REQUEST = new Schema[] { METADATA_REQUEST_V0 };
    public static Schema[] METADATA_RESPONSE = new Schema[] { METADATA_RESPONSE_V0 };

    /* Produce api */

    public static Schema TOPIC_PRODUCE_DATA_V0 = new Schema(new Field("topic", STRING),
                                                            new Field("data", new ArrayOf(new Schema(new Field("partition", INT32),
                                                                                                     new Field("record_set", BYTES)))));

    public static Schema PRODUCE_REQUEST_V0 = new Schema(new Field("acks",
                                                                   INT16,
                                                                   "The number of nodes that should replicate the produce before returning. -1 indicates the full ISR."),
                                                         new Field("timeout", INT32, "The time to await a response in ms."),
                                                         new Field("topic_data", new ArrayOf(TOPIC_PRODUCE_DATA_V0)));

    public static Schema PRODUCE_RESPONSE_V0 = new Schema(new Field("responses",
                                                                    new ArrayOf(new Schema(new Field("topic", STRING),
                                                                                           new Field("partition_responses",
                                                                                                     new ArrayOf(new Schema(new Field("partition",
                                                                                                                                      INT32),
                                                                                                                            new Field("error_code",
                                                                                                                                      INT16),
                                                                                                                            new Field("base_offset",
                                                                                                                                      INT64))))))));

    public static Schema[] PRODUCE_REQUEST = new Schema[] { PRODUCE_REQUEST_V0 };
    public static Schema[] PRODUCE_RESPONSE = new Schema[] { PRODUCE_RESPONSE_V0 };

    /* Offset commit api */
    public static Schema OFFSET_COMMIT_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                   INT32,
                                                                                   "Topic partition id."),
                                                                         new Field("offset",
                                                                                   INT64,
                                                                                   "Message offset to be committed."),
                                                                         new Field("metadata",
                                                                                   STRING,
                                                                                   "Any associated metadata the client wants to keep."));

    public static Schema OFFSET_COMMIT_REQUEST_PARTITION_V1 = new Schema(new Field("partition",
                                                                                   INT32,
                                                                                   "Topic partition id."),
                                                                         new Field("offset",
                                                                                   INT64,
                                                                                   "Message offset to be committed."),
                                                                         new Field("timestamp",
                                                                                   INT64,
                                                                                   "Timestamp of the commit"),
                                                                         new Field("metadata",
                                                                                   STRING,
                                                                                   "Any associated metadata the client wants to keep."));

    public static Schema OFFSET_COMMIT_REQUEST_TOPIC_V0 = new Schema(new Field("topic",
                                                                                STRING,
                                                                                "Topic to commit."),
                                                                       new Field("partitions",
                                                                                 new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V0),
                                                                                 "Partitions to commit offsets."));

    public static Schema OFFSET_COMMIT_REQUEST_TOPIC_V1 = new Schema(new Field("topic",
                                                                               STRING,
                                                                               "Topic to commit."),
                                                                     new Field("partitions",
                                                                               new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V1),
                                                                               "Partitions to commit offsets."));

    public static Schema OFFSET_COMMIT_REQUEST_V0 = new Schema(new Field("group_id",
                                                                         STRING,
                                                                         "The consumer group id."),
                                                               new Field("topics",
                                                                         new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V0),
                                                                         "Topics to commit offsets."));

    public static Schema OFFSET_COMMIT_REQUEST_V1 = new Schema(new Field("group_id",
                                                                         STRING,
                                                                         "The consumer group id."),
                                                               new Field("group_generation_id",
                                                                         INT32,
                                                                         "The generation of the consumer group."),
                                                               new Field("consumer_id",
                                                                         STRING,
                                                                         "The consumer id assigned by the group coordinator."),
                                                               new Field("topics",
                                                                         new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V1),
                                                                         "Topics to commit offsets."));

    public static Schema OFFSET_COMMIT_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                                    INT32,
                                                                                    "Topic partition id."),
                                                                          new Field("error_code",
                                                                                    INT16));

    public static Schema OFFSET_COMMIT_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                      new Field("partition_responses",
                                                                                new ArrayOf(OFFSET_COMMIT_RESPONSE_PARTITION_V0)));

    public static Schema OFFSET_COMMIT_RESPONSE_V0 = new Schema(new Field("responses",
                                                                          new ArrayOf(OFFSET_COMMIT_RESPONSE_TOPIC_V0)));

    /* The response types for both V0 and V1 of OFFSET_COMMIT_REQUEST are the same. */
    public static Schema OFFSET_COMMIT_RESPONSE_V1 = OFFSET_COMMIT_RESPONSE_V0;

    public static Schema[] OFFSET_COMMIT_REQUEST = new Schema[] { OFFSET_COMMIT_REQUEST_V0, OFFSET_COMMIT_REQUEST_V1 };
    public static Schema[] OFFSET_COMMIT_RESPONSE = new Schema[] { OFFSET_COMMIT_RESPONSE_V0, OFFSET_COMMIT_RESPONSE_V1};

    /* Offset fetch api */
    public static Schema OFFSET_FETCH_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                  INT32,
                                                                                  "Topic partition id."));

    public static Schema OFFSET_FETCH_REQUEST_TOPIC_V0 = new Schema(new Field("topic",
                                                                               STRING,
                                                                               "Topic to fetch offset."),
                                                                     new Field("partitions",
                                                                                new ArrayOf(OFFSET_FETCH_REQUEST_PARTITION_V0),
                                                                                "Partitions to fetch offsets."));

    public static Schema OFFSET_FETCH_REQUEST_V0 = new Schema(new Field("group_id",
                                                                        STRING,
                                                                        "The consumer group id."),
                                                              new Field("topics",
                                                                        new ArrayOf(OFFSET_FETCH_REQUEST_TOPIC_V0),
                                                                        "Topics to fetch offsets."));

    // version 0 and 1 have exactly the same wire format, but different functionality.
    // version 0 will read the offsets from ZK and version 1 will read the offsets from Kafka.
    public static Schema OFFSET_FETCH_REQUEST_V1 = OFFSET_FETCH_REQUEST_V0;

    public static Schema OFFSET_FETCH_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                                   INT32,
                                                                                   "Topic partition id."),
                                                                         new Field("offset",
                                                                                   INT64,
                                                                                   "Last committed message offset."),
                                                                         new Field("metadata",
                                                                                   STRING,
                                                                                   "Any associated metadata the client wants to keep."),
                                                                         new Field("error_code",
                                                                                   INT16));

    public static Schema OFFSET_FETCH_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                     new Field("partition_responses",
                                                                               new ArrayOf(OFFSET_FETCH_RESPONSE_PARTITION_V0)));

    public static Schema OFFSET_FETCH_RESPONSE_V0 = new Schema(new Field("responses",
                                                                         new ArrayOf(OFFSET_FETCH_RESPONSE_TOPIC_V0)));

    /* The response types for both V0 and V1 of OFFSET_FETCH_RESPONSE are the same. */
    public static Schema OFFSET_FETCH_RESPONSE_V1 = OFFSET_FETCH_RESPONSE_V0;

    public static Schema[] OFFSET_FETCH_REQUEST = new Schema[] { OFFSET_FETCH_REQUEST_V0, OFFSET_FETCH_REQUEST_V1 };
    public static Schema[] OFFSET_FETCH_RESPONSE = new Schema[] { OFFSET_FETCH_RESPONSE_V0, OFFSET_FETCH_RESPONSE_V1 };

    /* List offset api */
    public static Schema LIST_OFFSET_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                                 INT32,
                                                                                 "Topic partition id."),
                                                                       new Field("timestamp",
                                                                                 INT64,
                                                                                 "Timestamp."),
                                                                       new Field("max_num_offsets",
                                                                                 INT32,
                                                                                 "Maximum offsets to return."));

    public static Schema LIST_OFFSET_REQUEST_TOPIC_V0 = new Schema(new Field("topic",
                                                                             STRING,
                                                                             "Topic to list offset."),
                                                                   new Field("partitions",
                                                                             new ArrayOf(LIST_OFFSET_REQUEST_PARTITION_V0),
                                                                             "Partitions to list offset."));

    public static Schema LIST_OFFSET_REQUEST_V0 = new Schema(new Field("replica_id",
                                                                       INT32,
                                                                       "Broker id of the follower. For normal consumers, use -1."),
                                                             new Field("topics",
                                                                        new ArrayOf(LIST_OFFSET_REQUEST_TOPIC_V0),
                                                                        "Topics to list offsets."));

    public static Schema LIST_OFFSET_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                                  INT32,
                                                                                  "Topic partition id."),
                                                                        new Field("error_code",
                                                                                  INT16),
                                                                        new Field("offsets",
                                                                                  new ArrayOf(INT64),
                                                                                  "A list of offsets."));

    public static Schema LIST_OFFSET_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                    new Field("partition_responses",
                                                                              new ArrayOf(LIST_OFFSET_RESPONSE_PARTITION_V0)));

    public static Schema LIST_OFFSET_RESPONSE_V0 = new Schema(new Field("responses",
                                                                  new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V0)));

    public static Schema[] LIST_OFFSET_REQUEST = new Schema[] { LIST_OFFSET_REQUEST_V0 };
    public static Schema[] LIST_OFFSET_RESPONSE = new Schema[] { LIST_OFFSET_RESPONSE_V0 };

    /* Fetch api */
    public static Schema FETCH_REQUEST_PARTITION_V0 = new Schema(new Field("partition",
                                                                           INT32,
                                                                           "Topic partition id."),
                                                                 new Field("fetch_offset",
                                                                           INT64,
                                                                           "Message offset."),
                                                                 new Field("max_bytes",
                                                                           INT32,
                                                                           "Maximum bytes to fetch."));

    public static Schema FETCH_REQUEST_TOPIC_V0 = new Schema(new Field("topic",
                                                                       STRING,
                                                                       "Topic to fetch."),
                                                             new Field("partitions",
                                                                       new ArrayOf(FETCH_REQUEST_PARTITION_V0),
                                                                       "Partitions to fetch."));

    public static Schema FETCH_REQUEST_V0 = new Schema(new Field("replica_id",
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

    public static Schema FETCH_RESPONSE_PARTITION_V0 = new Schema(new Field("partition",
                                                                            INT32,
                                                                            "Topic partition id."),
                                                                  new Field("error_code",
                                                                            INT16),
                                                                  new Field("high_watermark",
                                                                            INT64,
                                                                            "Last committed offset."),
                                                                  new Field("record_set", BYTES));

    public static Schema FETCH_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                              new Field("partition_responses",
                                                                        new ArrayOf(FETCH_RESPONSE_PARTITION_V0)));

    public static Schema FETCH_RESPONSE_V0 = new Schema(new Field("responses",
                                                                  new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));

    public static Schema[] FETCH_REQUEST = new Schema[] { FETCH_REQUEST_V0 };
    public static Schema[] FETCH_RESPONSE = new Schema[] { FETCH_RESPONSE_V0 };

    /* Consumer metadata api */
    public static Schema CONSUMER_METADATA_REQUEST_V0 = new Schema(new Field("group_id",
                                                                             STRING,
                                                                             "The consumer group id."));

    public static Schema CONSUMER_METADATA_RESPONSE_V0 = new Schema(new Field("error_code",
                                                                              INT16),
                                                                    new Field("coordinator",
                                                                              BROKER,
                                                                              "Host and port information for the coordinator for a consumer group."));

    public static Schema[] CONSUMER_METADATA_REQUEST = new Schema[] { CONSUMER_METADATA_REQUEST_V0 };
    public static Schema[] CONSUMER_METADATA_RESPONSE = new Schema[] { CONSUMER_METADATA_RESPONSE_V0 };

    /* Join group api */
    public static Schema JOIN_GROUP_REQUEST_V0 = new Schema(new Field("group_id",
                                                                      STRING,
                                                                      "The consumer group id."),
                                                            new Field("session_timeout",
                                                                      INT32,
                                                                      "The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms."),
                                                            new Field("topics",
                                                                      new ArrayOf(STRING),
                                                                      "An array of topics to subscribe to."),
                                                            new Field("consumer_id",
                                                                      STRING,
                                                                      "The assigned consumer id or an empty string for a new consumer."),
                                                            new Field("partition_assignment_strategy",
                                                                      STRING,
                                                                      "The strategy for the coordinator to assign partitions."));

    public static Schema JOIN_GROUP_RESPONSE_TOPIC_V0 = new Schema(new Field("topic", STRING),
                                                                   new Field("partitions", new ArrayOf(INT32)));
    public static Schema JOIN_GROUP_RESPONSE_V0 = new Schema(new Field("error_code",
                                                                       INT16),
                                                             new Field("group_generation_id",
                                                                       INT32,
                                                                       "The generation of the consumer group."),
                                                             new Field("consumer_id",
                                                                       STRING,
                                                                       "The consumer id assigned by the group coordinator."),
                                                             new Field("assigned_partitions",
                                                                       new ArrayOf(JOIN_GROUP_RESPONSE_TOPIC_V0)));

    public static Schema[] JOIN_GROUP_REQUEST = new Schema[] { JOIN_GROUP_REQUEST_V0 };
    public static Schema[] JOIN_GROUP_RESPONSE = new Schema[] { JOIN_GROUP_RESPONSE_V0 };

    /* Heartbeat api */
    public static Schema HEARTBEAT_REQUEST_V0 = new Schema(new Field("group_id",
                                                                      STRING,
                                                                      "The consumer group id."),
                                                            new Field("group_generation_id",
                                                                      INT32,
                                                                      "The generation of the consumer group."),
                                                            new Field("consumer_id",
                                                                      STRING,
                                                                      "The consumer id assigned by the group coordinator."));

    public static Schema HEARTBEAT_RESPONSE_V0 = new Schema(new Field("error_code",
                                                                       INT16));

    public static Schema[] HEARTBEAT_REQUEST = new Schema[] {HEARTBEAT_REQUEST_V0};
    public static Schema[] HEARTBEAT_RESPONSE = new Schema[] {HEARTBEAT_RESPONSE_V0};

    /* an array of all requests and responses with all schema versions */
    public static Schema[][] REQUESTS = new Schema[ApiKeys.MAX_API_KEY + 1][];
    public static Schema[][] RESPONSES = new Schema[ApiKeys.MAX_API_KEY + 1][];

    /* the latest version of each api */
    public static short[] CURR_VERSION = new short[ApiKeys.MAX_API_KEY + 1];

    static {
        REQUESTS[ApiKeys.PRODUCE.id] = PRODUCE_REQUEST;
        REQUESTS[ApiKeys.FETCH.id] = FETCH_REQUEST;
        REQUESTS[ApiKeys.LIST_OFFSETS.id] = LIST_OFFSET_REQUEST;
        REQUESTS[ApiKeys.METADATA.id] = METADATA_REQUEST;
        REQUESTS[ApiKeys.LEADER_AND_ISR.id] = new Schema[] {};
        REQUESTS[ApiKeys.STOP_REPLICA.id] = new Schema[] {};
        REQUESTS[ApiKeys.OFFSET_COMMIT.id] = OFFSET_COMMIT_REQUEST;
        REQUESTS[ApiKeys.OFFSET_FETCH.id] = OFFSET_FETCH_REQUEST;
        REQUESTS[ApiKeys.CONSUMER_METADATA.id] = CONSUMER_METADATA_REQUEST;
        REQUESTS[ApiKeys.JOIN_GROUP.id] = JOIN_GROUP_REQUEST;
        REQUESTS[ApiKeys.HEARTBEAT.id] = HEARTBEAT_REQUEST;

        RESPONSES[ApiKeys.PRODUCE.id] = PRODUCE_RESPONSE;
        RESPONSES[ApiKeys.FETCH.id] = FETCH_RESPONSE;
        RESPONSES[ApiKeys.LIST_OFFSETS.id] = LIST_OFFSET_RESPONSE;
        RESPONSES[ApiKeys.METADATA.id] = METADATA_RESPONSE;
        RESPONSES[ApiKeys.LEADER_AND_ISR.id] = new Schema[] {};
        RESPONSES[ApiKeys.STOP_REPLICA.id] = new Schema[] {};
        RESPONSES[ApiKeys.OFFSET_COMMIT.id] = OFFSET_COMMIT_RESPONSE;
        RESPONSES[ApiKeys.OFFSET_FETCH.id] = OFFSET_FETCH_RESPONSE;
        RESPONSES[ApiKeys.CONSUMER_METADATA.id] = CONSUMER_METADATA_RESPONSE;
        RESPONSES[ApiKeys.JOIN_GROUP.id] = JOIN_GROUP_RESPONSE;
        RESPONSES[ApiKeys.HEARTBEAT.id] = HEARTBEAT_RESPONSE;

        /* set the maximum version of each api */
        for (ApiKeys api : ApiKeys.values())
            CURR_VERSION[api.id] = (short) (REQUESTS[api.id].length - 1);

        /* sanity check that we have the same number of request and response versions for each api */
        for (ApiKeys api : ApiKeys.values())
            if (REQUESTS[api.id].length != RESPONSES[api.id].length)
                throw new IllegalStateException(REQUESTS[api.id].length + " request versions for api "
                                                + api.name
                                                + " but "
                                                + RESPONSES[api.id].length
                                                + " response versions.");
    }

}
