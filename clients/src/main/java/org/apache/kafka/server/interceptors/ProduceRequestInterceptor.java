package org.apache.kafka.server.interceptors;

import org.apache.kafka.common.header.Header;

/**
 * ProduceRequestInterceptors can be defined to perform custom, light-weight processing on every record received by a
 * broker.
 *
 * Broker-side interceptors should be used with caution:
 *  1. Processing messages that were sent in compressed format by the producer will need to be decompressed and then
 *    re-compressed to perform broker-side processing
 *  2. Performing unduly long or complex computations can negatively impact overall cluster health and performance
 *
 * Potential use cases:
 *   - Schema validation
 *   - Privacy enforcement
 *   - Decoupling server-side and client-side serialization
 */
public abstract class ProduceRequestInterceptor {
    // Custom function for mutating the original message. If the method returns a ProduceRequestInterceptorSkipRecordException,
    // the record will be removed from the batch and won't be persisted in the target log. All other exceptions are
    // considered "fatal" and will result in a request error
    public abstract ProduceRequestInterceptorResult processRecord(byte[] key, byte[] value, String topic, int partition, Header[] headers);

    // Method that gets called during the interceptor's initialization to configure itself
    public abstract void configure();
}
