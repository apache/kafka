package org.apache.kafka.common.test.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ClusterTestExtensions.class)
public class ClusterTestBeforeEachTest {
    private final ClusterInstance clusterInstance;

    ClusterTestBeforeEachTest(ClusterInstance clusterInstance) {     // Constructor injections
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    void before() {
        assertNotNull(clusterInstance);
        if (!clusterInstance.started()) {
            clusterInstance.start();
        }
        assertDoesNotThrow(clusterInstance::waitForReadyBrokers);
    }

    @ClusterTest
    public void testDefault() {
        assertTrue(true);
        assertNotNull(clusterInstance);
    }

    @ClusterTest(autoStart = AutoStart.NO)
    public void testNoAutoStart() {
        assertTrue(true);
        assertNotNull(clusterInstance);
    }
}
