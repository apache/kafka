package kafka.common.record;

/**
 * An offset and record pair
 */
public final class LogEntry {

    private final long offset;
    private final Record record;

    public LogEntry(long offset, Record record) {
        this.offset = offset;
        this.record = record;
    }

    public long offset() {
        return this.offset;
    }

    public Record record() {
        return this.record;
    }

    @Override
    public String toString() {
        return "LogEntry(" + offset + ", " + record + ")";
    }
}
