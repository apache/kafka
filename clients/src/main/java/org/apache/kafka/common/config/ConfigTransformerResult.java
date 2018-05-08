package org.apache.kafka.common.config;

import java.util.Map;

/**
 * The result of a transformation from {@link ConfigTransformer}.
 */
public class ConfigTransformerResult {

    private Map<String, Long> ttls;
    private Map<String, String> data;

    /**
     * Creates a new ConfigTransformerResult with the given data and TTL values for a set of paths.
     *
     * @param data a Map of key-value pairs
     * @param ttls a Map of path and TTL values (in milliseconds)
     */
    public ConfigTransformerResult(Map<String, String> data, Map<String, Long> ttls) {
        this.data = data;
        this.ttls = ttls;
    }

    /**
     * Returns the data.
     *
     * @return data a Map of key-value pairs
     */
    public Map<String, String> data() {
        return data;
    }

    /**
     * Returns the TTL values (in milliseconds).
     *
     * @return data a Map of path and TTL values
     */
    public Map<String, Long> ttls() {
        return ttls;
    }
}
