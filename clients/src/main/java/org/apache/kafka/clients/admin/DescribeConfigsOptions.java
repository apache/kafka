package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for describeConfigs.
 */
@InterfaceStability.Unstable
public class DescribeConfigsOptions {
    private Integer timeoutMs = null;

    public DescribeConfigsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public Integer timeoutMs() {
        return timeoutMs;
    }
}
