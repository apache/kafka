package org.apache.kafka.common.protocol;

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class RecordsWriter implements Writable {
    private final String dest;
    private final Consumer<Send> sendConsumer;
    private final ByteArrayOutputStream byteArrayOutputStream;
    private final DataOutput output;

    public RecordsWriter(String dest, Consumer<Send> sendConsumer) {
        this.dest = dest;
        this.sendConsumer = sendConsumer;
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.output = new DataOutputStream(this.byteArrayOutputStream);
    }

    @Override
    public void writeByte(byte val) {
        writeQuietly(() -> output.writeByte(val));
    }

    @Override
    public void writeShort(short val) {
        writeQuietly(() -> output.writeShort(val));
    }

    @Override
    public void writeInt(int val) {
        writeQuietly(() -> output.writeInt(val));
    }

    @Override
    public void writeLong(long val) {
        writeQuietly(() -> output.writeLong(val));

    }

    @Override
    public void writeDouble(double val) {
        writeQuietly(() -> ByteUtils.writeDouble(val, output));

    }

    @Override
    public void writeByteArray(byte[] arr) {
        writeQuietly(() -> output.write(arr));
    }

    @Override
    public void writeUnsignedVarint(int i) {
        writeQuietly(() -> ByteUtils.writeUnsignedVarint(i, output));
    }

    @Override
    public void writeByteBuffer(ByteBuffer src) {
        writeQuietly(() -> output.write(src.array(), src.position(), src.remaining()));
    }

    @FunctionalInterface
    private interface IOExceptionThrowingRunnable {
        void run() throws IOException;
    }

    private void writeQuietly(IOExceptionThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (IOException e) {
            throw new RuntimeException("Writable encountered an IO error", e);
        }
    }

    @Override
    public void writeRecords(BaseRecords records) {
        flush();
        sendConsumer.accept(records.toSend(dest));
    }

    /**
     * Flush any pending bytes as a ByteBufferSend and reset the buffer
     */
    public void flush() {
        // Flush buffered bytes and reset buffer
        ByteBufferSend send = new ByteBufferSend(dest,
                ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
        sendConsumer.accept(send);
        byteArrayOutputStream.reset();
    }
}
