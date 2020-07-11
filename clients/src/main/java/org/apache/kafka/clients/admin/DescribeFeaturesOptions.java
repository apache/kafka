package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

@InterfaceStability.Evolving
public class DescribeFeaturesOptions extends AbstractOptions<DescribeFeaturesOptions> {
    /**
     * - True means the {@link Admin#describeFeatures(DescribeFeaturesOptions)} request can be
     *   issued only to the controller.
     * - False means the {@link Admin#describeFeatures(DescribeFeaturesOptions)} request can be
     *   issued to any random broker.
     */
    private boolean shouldSendRequestToController = false;

    /**
     * Sets a flag indicating that the describe features request should be issued to the controller.
     */
    public DescribeFeaturesOptions sendRequestToController(boolean shouldSendRequestToController) {
        this.shouldSendRequestToController = shouldSendRequestToController;
        return this;
    }

    public boolean sendRequestToController() {
        return shouldSendRequestToController;
    }

    /**
     * Sets the timeout in milliseconds for this operation or {@code null} if the default API
     * timeout for the AdminClient should be used.
     */
    public DescribeFeaturesOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }
}
