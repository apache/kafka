package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Objects;
import java.util.Optional;

/**
 * Class to extract group rebalance related configs.
 */
public class GroupRebalanceConfig {

    public final int sessionTimeoutMs;
    public final int rebalanceTimeoutMs;
    public final int heartbeatIntervalMs;
    public final String groupId;
    public final Optional<String> groupInstanceId;
    public final long retryBackoffMs;
    public final boolean leaveGroupOnClose;

    public GroupRebalanceConfig(AbstractConfig config) {
        this.sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG);
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.groupId = Objects.requireNonNull(config.getString(CommonClientConfigs.GROUP_ID_CONFIG),
                "Expected a non-null group id for coordinator construction");
        String groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG);
        if (groupInstanceId != null) {
            JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
            this.groupInstanceId = Optional.of(groupInstanceId);
        } else {
            this.groupInstanceId = Optional.empty();
        }
        this.retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
        this.leaveGroupOnClose = config.getBoolean("internal.leave.group.on.close");
    }
}
