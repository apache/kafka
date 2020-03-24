package kafka.log;

import java.util.Objects;

public enum SegmentFile {

    LOG("log"), OFFSET_INDEX("index"), TIME_INDEX("timeindex"),
    TXN_INDEX("txnindex"), STATUS("status");

    private final String name;
    SegmentFile(String name) {
        this.name = Objects.requireNonNull(name);
    }

    public String getName() {
        return name;
    }
}
