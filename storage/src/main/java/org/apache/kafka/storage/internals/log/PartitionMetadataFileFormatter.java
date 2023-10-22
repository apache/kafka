package org.apache.kafka.storage.internals.log;

public class PartitionMetadataFileFormatter {
    public static String toFile(PartitionMetadata data) {
        return "version: " + data.version + "\ntopic_id: " + data.topicId;
    }
}
