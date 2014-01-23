package kafka.common;

import java.util.Map;

/**
 * A Mix-in style interface for classes that are instantiated by reflection and need to take configuration parameters
 */
public interface Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    public void configure(Map<String, ?> configs);

}
