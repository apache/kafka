package org.apache.kafka.clients.consumer;

import java.time.Duration;

/**
 * This exception is raised when an offset commit with {@link KafkaConsumer#commitSync()} fails
 * since the consumer group is rebalancing. This can happen when a group has started, but not finished
 * rebalancing before the commit is issued. In this case, callers could trigger {@link KafkaConsumer#poll(Duration)}
 * to complete the rebalance and then retry committing those offsets whose partitions are still assigned to
 * the consumer instance after the rebalance.
 */
public class RebalanceInProgressException extends RetriableCommitFailedException {

    private static final long serialVersionUID = 1L;

    public RebalanceInProgressException(final String message) {
        super(message);
    }

    public RebalanceInProgressException() {
        super("Commit cannot be completed since he consumer group is rebalancing. This can happen when a group has started, " +
            "but not finished rebalancing before the commit is issued. You can try completing the rebalance " +
            "by calling poll() and then retry committing those offsets whose partitions are still assigned to the " +
            "consumer after the rebalance");
    }
}
