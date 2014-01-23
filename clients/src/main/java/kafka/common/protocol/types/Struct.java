package kafka.common.protocol.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A record that can be serialized and deserialized according to a pre-defined schema
 */
public class Struct {
    private final Schema schema;
    private final Object[] values;

    Struct(Schema schema, Object[] values) {
        this.schema = schema;
        this.values = values;
    }

    public Struct(Schema schema) {
        this.schema = schema;
        this.values = new Object[this.schema.numFields()];
    }

    /**
     * The schema for this struct.
     */
    public Schema schema() {
        return this.schema;
    }

    /**
     * Return the value of the given pre-validated field, or if the value is missing return the default value.
     * 
     * @param field The field for which to get the default value
     * @throws SchemaException if the field has no value and has no default.
     */
    private Object getFieldOrDefault(Field field) {
        Object value = this.values[field.index];
        if (value != null)
            return value;
        else if (field.defaultValue != Field.NO_DEFAULT)
            return field.defaultValue;
        else
            throw new SchemaException("Missing value for field '" + field.name + " which has no default value.");
    }

    /**
     * Get the value for the field directly by the field index with no lookup needed (faster!)
     * 
     * @param field The field to look up
     * @return The value for that field.
     */
    public Object get(Field field) {
        validateField(field);
        return getFieldOrDefault(field);
    }

    /**
     * Get the record value for the field with the given name by doing a hash table lookup (slower!)
     * 
     * @param name The name of the field
     * @return The value in the record
     */
    public Object get(String name) {
        Field field = schema.get(name);
        if (field == null)
            throw new SchemaException("No such field: " + name);
        return getFieldOrDefault(field);
    }

    public Struct getStruct(Field field) {
        return (Struct) get(field);
    }

    public Struct getStruct(String name) {
        return (Struct) get(name);
    }

    public Short getShort(Field field) {
        return (Short) get(field);
    }

    public Short getShort(String name) {
        return (Short) get(name);
    }

    public Integer getInt(Field field) {
        return (Integer) get(field);
    }

    public Integer getInt(String name) {
        return (Integer) get(name);
    }

    public Object[] getArray(Field field) {
        return (Object[]) get(field);
    }

    public Object[] getArray(String name) {
        return (Object[]) get(name);
    }

    public String getString(Field field) {
        return (String) get(field);
    }

    public String getString(String name) {
        return (String) get(name);
    }

    /**
     * Set the given field to the specified value
     * 
     * @param field The field
     * @param value The value
     */
    public Struct set(Field field, Object value) {
        validateField(field);
        this.values[field.index] = value;
        return this;
    }

    /**
     * Set the field specified by the given name to the value
     * 
     * @param name The name of the field
     * @param value The value to set
     */
    public Struct set(String name, Object value) {
        Field field = this.schema.get(name);
        if (field == null)
            throw new SchemaException("Unknown field: " + name);
        this.values[field.index] = value;
        return this;
    }

    /**
     * Create a struct for the schema of a container type (struct or array)
     * 
     * @param field The field to create an instance of
     * @return The struct
     */
    public Struct instance(Field field) {
        validateField(field);
        if (field.type() instanceof Schema) {
            return new Struct((Schema) field.type());
        } else if (field.type() instanceof ArrayOf) {
            ArrayOf array = (ArrayOf) field.type();
            return new Struct((Schema) array.type());
        } else {
            throw new SchemaException("Field " + field.name + " is not a container type, it is of type " + field.type());
        }
    }

    /**
     * Create a struct instance for the given field which must be a container type (struct or array)
     * 
     * @param field The name of the field to create (field must be a schema type)
     * @return The struct
     */
    public Struct instance(String field) {
        return instance(schema.get(field));
    }

    /**
     * Empty all the values from this record
     */
    public void clear() {
        Arrays.fill(this.values, null);
    }

    /**
     * Get the serialized size of this object
     */
    public int sizeOf() {
        return this.schema.sizeOf(this);
    }

    /**
     * Write this struct to a buffer
     */
    public void writeTo(ByteBuffer buffer) {
        this.schema.write(buffer, this);
    }

    /**
     * Ensure the user doesn't try to access fields from the wrong schema
     */
    private void validateField(Field field) {
        if (this.schema != field.schema)
            throw new SchemaException("Attempt to access field '" + field.name + " from a different schema instance.");
        if (field.index > values.length)
            throw new SchemaException("Invalid field index: " + field.index);
    }

    /**
     * Validate the contents of this struct against its schema
     */
    public void validate() {
        this.schema.validate(this);
    }

    /**
     * Create a byte buffer containing the serialized form of the values in this struct. This method can choose to break
     * the struct into multiple ByteBuffers if need be.
     */
    public ByteBuffer[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(sizeOf());
        writeTo(buffer);
        return new ByteBuffer[] { buffer };
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (int i = 0; i < this.values.length; i++) {
            b.append(this.schema.get(i).name);
            b.append('=');
            b.append(this.values[i]);
            if (i < this.values.length - 1)
                b.append(',');
        }
        b.append('}');
        return b.toString();
    }

}
