package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

public class SubscriptionResponseWrapper<FV> {
    final private long[] originalValueHash;
    final private FV foreignValue;

    public SubscriptionResponseWrapper(long[] originalValueHash, FV foreignValue) {
        this.originalValueHash = originalValueHash;
        this.foreignValue = foreignValue;
    }

    public FV getForeignValue() {
        return foreignValue;
    }

    public long[] getOriginalValueHash() {
        return originalValueHash;
    }
}
