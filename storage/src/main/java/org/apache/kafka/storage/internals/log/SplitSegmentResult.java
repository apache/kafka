package org.apache.kafka.storage.internals.log;

/**
 * Holds the result of splitting a segment into one or more segments, see LocalLog.splitOverflowedSegment().
 */
public class SplitSegmentResult {
    private Iterable<LogSegment> deletedSegments;
    private Iterable<LogSegment> newSegments;

    /**
     * Create a SplitSegmentResult with the provided parameters.
     *
     * @param deletedSegments segments deleted when splitting a segment
     * @param newSegments new segments created when splitting a segment
     */
    public SplitSegmentResult(Iterable<LogSegment> deletedSegments, Iterable<LogSegment> newSegments) {
        this.deletedSegments = deletedSegments;
        this.newSegments = newSegments;
    }

    public Iterable<LogSegment> getDeletedSegments() {
        return deletedSegments;
    }

    public Iterable<LogSegment> getNewSegments() {
        return newSegments;
    }
}
