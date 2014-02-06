package kafka.common.config;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.common.config.ConfigDef.Range;
import kafka.common.config.ConfigDef.Type;

import org.junit.Test;

public class ConfigDefTest {

    @Test
    public void testBasicTypes() {
        ConfigDef def = new ConfigDef().define("a", Type.INT, 5, Range.between(0, 14), "docs")
                                       .define("b", Type.LONG, "docs")
                                       .define("c", Type.STRING, "hello", "docs")
                                       .define("d", Type.LIST, "docs")
                                       .define("e", Type.DOUBLE, "docs")
                                       .define("f", Type.CLASS, "docs")
                                       .define("g", Type.BOOLEAN, "docs");

        Properties props = new Properties();
        props.put("a", "1   ");
        props.put("b", 2);
        props.put("d", " a , b, c");
        props.put("e", 42.5d);
        props.put("f", String.class.getName());
        props.put("g", "true");

        Map<String, Object> vals = def.parse(props);
        assertEquals(1, vals.get("a"));
        assertEquals(2L, vals.get("b"));
        assertEquals("hello", vals.get("c"));
        assertEquals(asList("a", "b", "c"), vals.get("d"));
        assertEquals(42.5d, vals.get("e"));
        assertEquals(String.class, vals.get("f"));
        assertEquals(true, vals.get("g"));
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDefault() {
        new ConfigDef().define("a", Type.INT, "hello", "docs");
    }

    @Test(expected = ConfigException.class)
    public void testNullDefault() {
        new ConfigDef().define("a", Type.INT, null, null, "docs");
    }

    @Test(expected = ConfigException.class)
    public void testMissingRequired() {
        new ConfigDef().define("a", Type.INT, "docs").parse(new HashMap<String, Object>());
    }

    @Test(expected = ConfigException.class)
    public void testDefinedTwice() {
        new ConfigDef().define("a", Type.STRING, "docs").define("a", Type.INT, "docs");
    }

    @Test
    public void testBadInputs() {
        testBadInputs(Type.INT, "hello", null, "42.5", 42.5, Long.MAX_VALUE, Long.toString(Long.MAX_VALUE), new Object());
        testBadInputs(Type.LONG, "hello", null, "42.5", Long.toString(Long.MAX_VALUE) + "00", new Object());
        testBadInputs(Type.DOUBLE, "hello", null, new Object());
        testBadInputs(Type.STRING, new Object());
        testBadInputs(Type.LIST, 53, new Object());
    }

    private void testBadInputs(Type type, Object... values) {
        for (Object value : values) {
            Map<String, Object> m = new HashMap<String, Object>();
            m.put("name", value);
            ConfigDef def = new ConfigDef().define("name", type, "docs");
            try {
                def.parse(m);
                fail("Expected a config exception on bad input for value " + value);
            } catch (ConfigException e) {
                // this is good
            }
        }
    }
}
