package org.apache.kafka.common.protocol;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;

public class DataInputStreamReadable extends DataInputReadable implements Closeable {
    public DataInputStreamReadable(DataInputStream input) {
        super(input);
    }

    @Override
    public void close() {
        try {
            ((DataInputStream) input).close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
