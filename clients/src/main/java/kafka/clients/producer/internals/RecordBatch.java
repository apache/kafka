package kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;

import kafka.clients.producer.Callback;
import kafka.clients.producer.RecordSend;
import kafka.common.TopicPartition;
import kafka.common.record.CompressionType;
import kafka.common.record.MemoryRecords;

/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {
    public int recordCount = 0;
    public final long created;
    public final MemoryRecords records;
    public final TopicPartition topicPartition;
    private final ProduceRequestResult produceFuture;
    private final List<Thunk> thunks;

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.created = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
    }

    /**
     * Append the message to the current message set and return the relative offset within that message set
     * 
     * @return The RecordSend corresponding to this message or null if there isn't sufficient room.
     */
    public RecordSend tryAppend(byte[] key, byte[] value, CompressionType compression, Callback callback) {
        if (!this.records.hasRoomFor(key, value)) {
            return null;
        } else {
            this.records.append(0L, key, value, compression);
            RecordSend send = new RecordSend(this.recordCount++, this.produceFuture);
            if (callback != null)
                thunks.add(new Thunk(callback, send));
            return send;
        }
    }

    /**
     * Complete the request
     * 
     * @param offset The offset
     * @param errorCode The error code or 0 if no error
     */
    public void done(long offset, RuntimeException exception) {
        this.produceFuture.done(topicPartition, offset, exception);
        // execute callbacks
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                this.thunks.get(i).execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A callback and the associated RecordSend argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final RecordSend send;

        public Thunk(Callback callback, RecordSend send) {
            this.callback = callback;
            this.send = send;
        }

        public void execute() {
            this.callback.onCompletion(this.send);
        }
    }
}