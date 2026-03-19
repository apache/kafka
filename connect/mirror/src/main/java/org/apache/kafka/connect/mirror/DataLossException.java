/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.apache.kafka.connect.mirror;

/**
 * Thrown by {@link MirrorSourceTask} when a log truncation is detected.
 *
 * <p>Log truncation happens when Kafka's retention policy deletes topic segments
 * that MirrorMaker 2 has not yet replicated.  The consumer position falls below
 * the topic's {@code logStartOffset}, meaning those messages are gone forever.
 *
 * <p>This is an unrecoverable situation from MirrorMaker 2's perspective – we
 * cannot replay data that no longer exists on the source.  We therefore throw
 * this exception to <em>fail fast</em>, surfacing the problem immediately rather
 * than silently creating a gap in the DR cluster.
 *
 * <p>Operators should:
 * <ol>
 *   <li>Check the MirrorMaker 2 logs for the offset range of the lost data.</li>
 *   <li>Assess the impact on the DR cluster's state.</li>
 *   <li>Either reset the consumer to the new logStartOffset (accepting the gap)
 *       or restore the source data from a backup before restarting replication.</li>
 * </ol>
 */
public class DataLossException extends RuntimeException {

    public DataLossException(String message) {
        super(message);
    }

    public DataLossException(String message, Throwable cause) {
        super(message, cause);
    }
}
