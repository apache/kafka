package org.apache.kafka.common.errors;

public class OffsetOutOfRangeExceptionWithOffsetValues extends OffsetOutOfRangeException {

    private final long startOffset;
    private final long logStartOffset;
    private final long lastStableOffset;

    public long getStartOffset() {
        return startOffset;
    }

    public long getLastStableOffset() {
        return lastStableOffset;
    }

    public long getLogStartOffset() { return logStartOffset; }

    public OffsetOutOfRangeExceptionWithOffsetValues(String message, long startOffset, long logStartOffset, long lastStableOffset) {
        super(message);
        this.startOffset = startOffset;
        this.logStartOffset = logStartOffset;
        this.lastStableOffset = lastStableOffset;
    }

}
