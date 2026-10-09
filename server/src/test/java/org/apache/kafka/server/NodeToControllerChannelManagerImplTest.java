

package org.apache.kafka.server;

import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NodeToControllerChannelManagerImplTest {

    @Test
    void testSelectorMetricsTagsForControllerOnlyNode() {
        AbstractKafkaConfig config = mock(AbstractKafkaConfig.class);
        when(config.getList(KRaftConfigs.PROCESS_ROLES_CONFIG)).thenReturn(List.of("controller"));
        when(config.nodeId()).thenReturn(123);

        assertEquals(Map.of("NodeId", "123"), NodeToControllerChannelManagerImpl.selectorMetricTags(config));
        verify(config, never()).brokerId();
    }

    @Test
    void testSelectorMetricsTagsForBrokerOnlyNode() {
        AbstractKafkaConfig config = mock(AbstractKafkaConfig.class);
        when(config.getList(KRaftConfigs.PROCESS_ROLES_CONFIG)).thenReturn(List.of("broker"));
        when(config.brokerId()).thenReturn(12);

        assertEquals(Map.of("BrokerId", "12"), NodeToControllerChannelManagerImpl.selectorMetricTags(config));
    }

    @Test
    void testSelectorMetricsTagsForCombinedRoleNode() {
        AbstractKafkaConfig config = mock(AbstractKafkaConfig.class);
        when(config.getList(KRaftConfigs.PROCESS_ROLES_CONFIG)).thenReturn(List.of("broker", "controller"));
        when(config.brokerId()).thenReturn(98);

        assertEquals(Map.of("BrokerId", "98"), NodeToControllerChannelManagerImpl.selectorMetricTags(config));
    }
}
