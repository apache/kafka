package kafka.common.protocol.types;

import java.nio.ByteBuffer;

/**
 * Represents a type for an array of a particular type
 */
public class ArrayOf extends Type {

    private final Type type;

    public ArrayOf(Type type) {
        this.type = type;
    }

    @Override
    public void write(ByteBuffer buffer, Object o) {
        Object[] objs = (Object[]) o;
        int size = objs.length;
        buffer.putInt(size);
        for (int i = 0; i < size; i++)
            type.write(buffer, objs[i]);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int size = buffer.getInt();
        Object[] objs = new Object[size];
        for (int i = 0; i < size; i++)
            objs[i] = type.read(buffer);
        return objs;
    }

    @Override
    public int sizeOf(Object o) {
        Object[] objs = (Object[]) o;
        int size = 4;
        for (int i = 0; i < objs.length; i++)
            size += type.sizeOf(objs[i]);
        return size;
    }

    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return "ARRAY(" + type + ")";
    }

    @Override
    public Object[] validate(Object item) {
        try {
            Object[] array = (Object[]) item;
            for (int i = 0; i < array.length; i++)
                type.validate(array[i]);
            return array;
        } catch (ClassCastException e) {
            throw new SchemaException("Not an Object[].");
        }
    }
}
