package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

@InterfaceStability.Evolving
public class UpdateFinalizedFeaturesOptions extends AbstractOptions<UpdateFinalizedFeaturesOptions> {
    /**
     * Sets the timeout in milliseconds for this operation or {@code null} if the default API
     * timeout for the AdminClient should be used.
     */
    public UpdateFinalizedFeaturesOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }
}
