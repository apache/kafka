package kafka.common.protocol.types;

/**
 * A field in a schema
 */
public class Field {

    public static final Object NO_DEFAULT = new Object();

    final int index;
    public final String name;
    public final Type type;
    public final Object defaultValue;
    public final String doc;
    final Schema schema;

    public Field(int index, String name, Type type, String doc, Object defaultValue, Schema schema) {
        this.index = index;
        this.name = name;
        this.type = type;
        this.doc = doc;
        this.defaultValue = defaultValue;
        this.schema = schema;
        if (defaultValue != NO_DEFAULT)
            type.validate(defaultValue);
    }

    public Field(int index, String name, Type type, String doc, Object defaultValue) {
        this(index, name, type, doc, defaultValue, null);
    }

    public Field(String name, Type type, String doc, Object defaultValue) {
        this(-1, name, type, doc, defaultValue);
    }

    public Field(String name, Type type, String doc) {
        this(name, type, doc, NO_DEFAULT);
    }

    public Field(String name, Type type) {
        this(name, type, "");
    }

    public Type type() {
        return type;
    }

}
