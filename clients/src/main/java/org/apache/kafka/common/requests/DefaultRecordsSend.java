package org.apache.kafka.common.requests;

import org.apache.kafka.common.record.Records;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public class DefaultRecordsSend extends RecordsSend {
    public DefaultRecordsSend(String destination, Records records) {
        super(destination, records);
    }

    @Override
    protected long writeRecordsTo(GatheringByteChannel channel, long position, int length) throws IOException {
        return records().writeTo(channel, position, length);
    }

    @Override
    protected Records records() {
        return (Records) super.records();
    }
}
