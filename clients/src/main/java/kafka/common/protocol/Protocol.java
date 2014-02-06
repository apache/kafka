package kafka.common.protocol;

import static kafka.common.protocol.types.Type.BYTES;
import static kafka.common.protocol.types.Type.INT16;
import static kafka.common.protocol.types.Type.INT32;
import static kafka.common.protocol.types.Type.INT64;
import static kafka.common.protocol.types.Type.STRING;
import kafka.common.protocol.types.ArrayOf;
import kafka.common.protocol.types.Field;
import kafka.common.protocol.types.Schema;

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

    /* an array of all requests and responses with all schema versions */
    public static Schema[][] REQUESTS = new Schema[ApiKeys.MAX_API_KEY + 1][];
    public static Schema[][] RESPONSES = new Schema[ApiKeys.MAX_API_KEY + 1][];

    /* the latest version of each api */
    public static short[] CURR_VERSION = new short[ApiKeys.MAX_API_KEY + 1];

    static {
        REQUESTS[ApiKeys.PRODUCE.id] = PRODUCE_REQUEST;
        REQUESTS[ApiKeys.FETCH.id] = new Schema[] {};
        REQUESTS[ApiKeys.LIST_OFFSETS.id] = new Schema[] {};
        REQUESTS[ApiKeys.METADATA.id] = METADATA_REQUEST;
        REQUESTS[ApiKeys.LEADER_AND_ISR.id] = new Schema[] {};
        REQUESTS[ApiKeys.STOP_REPLICA.id] = new Schema[] {};
        REQUESTS[ApiKeys.OFFSET_COMMIT.id] = new Schema[] {};
        REQUESTS[ApiKeys.OFFSET_FETCH.id] = new Schema[] {};

        RESPONSES[ApiKeys.PRODUCE.id] = PRODUCE_RESPONSE;
        RESPONSES[ApiKeys.FETCH.id] = new Schema[] {};
        RESPONSES[ApiKeys.LIST_OFFSETS.id] = new Schema[] {};
        RESPONSES[ApiKeys.METADATA.id] = METADATA_RESPONSE;
        RESPONSES[ApiKeys.LEADER_AND_ISR.id] = new Schema[] {};
        RESPONSES[ApiKeys.STOP_REPLICA.id] = new Schema[] {};
        RESPONSES[ApiKeys.OFFSET_COMMIT.id] = new Schema[] {};
        RESPONSES[ApiKeys.OFFSET_FETCH.id] = new Schema[] {};

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
