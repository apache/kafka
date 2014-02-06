package kafka.common.protocol;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "produce"),
    FETCH(1, "fetch"),
    LIST_OFFSETS(2, "list_offsets"),
    METADATA(3, "metadata"),
    LEADER_AND_ISR(4, "leader_and_isr"),
    STOP_REPLICA(5, "stop_replica"),
    OFFSET_COMMIT(6, "offset_commit"),
    OFFSET_FETCH(7, "offset_fetch");

    public static int MAX_API_KEY = 0;

    static {
        for (ApiKeys key : ApiKeys.values()) {
            MAX_API_KEY = Math.max(MAX_API_KEY, key.id);
        }
    }

    /** the perminant and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    private ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

}