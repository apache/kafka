package kafka.common.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.common.Configurable;
import kafka.common.KafkaException;
import kafka.common.utils.Utils;

/**
 * A convenient base class for configurations to extend.
 * <p>
 * This class holds both the original configuration that was provided as well as the parsed
 */
public class AbstractConfig {

    private final Set<String> used;
    private final Map<String, Object> values;
    private final Map<String, ?> originals;

    @SuppressWarnings("unchecked")
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals) {
        /* check that all the keys are really strings */
        for (Object key : originals.keySet())
            if (!(key instanceof String))
                throw new ConfigException(key.toString(), originals.get(key), "Key must be a string.");
        this.originals = (Map<String, ?>) originals;
        this.values = definition.parse(this.originals);
        this.used = Collections.synchronizedSet(new HashSet<String>());
    }

    protected Object get(String key) {
        if (!values.containsKey(key))
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        used.add(key);
        return values.get(key);
    }

    public int getInt(String key) {
        return (Integer) get(key);
    }

    public Long getLong(String key) {
        return (Long) get(key);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return (List<String>) get(key);
    }

    public boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public Class<?> getClass(String key) {
        return (Class<?>) get(key);
    }

    public Set<String> unused() {
        Set<String> keys = new HashSet<String>(originals.keySet());
        keys.remove(used);
        return keys;
    }

    /**
     * Get a configured instance of the give class specified by the given configuration key. If the object implements
     * Configurable configure it using the configuration.
     * 
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @return A configured instance of the class
     */
    public <T> T getConfiguredInstance(String key, Class<T> t) {
        Class<?> c = getClass(key);
        if (c == null)
            return null;
        Object o = Utils.newInstance(c);
        if (!t.isInstance(o))
            throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
        if (o instanceof Configurable)
            ((Configurable) o).configure(this.originals);
        return t.cast(o);
    }

}
