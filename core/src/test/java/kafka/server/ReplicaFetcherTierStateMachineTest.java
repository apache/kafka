/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server;

import kafka.log.UnifiedLog;
import kafka.log.remote.RemoteLogManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.junit.jupiter.api.Test;
import scala.Option;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ReplicaFetcherTierStateMachineTest {

    private final TopicPartition t1p0 = new TopicPartition("topic1", 0);

    @Test
    public void buildRemoteLogAuxStateShouldThrowErrorForUninitializedPartition() throws RemoteStorageException {
        LeaderEpochFileCache leaderEpochCache = createLeaderEpochCache(t1p0);
        leaderEpochCache.assign(0, 0);
        leaderEpochCache.assign(1, 1);
        leaderEpochCache.assign(3, 1);

        UnifiedLog log = mock(UnifiedLog.class);
        when(log.config()).thenReturn(createLogConfig());
        when(log.remoteLogEnabled()).thenReturn(true);

        RemoteLogManager rlm = mock(RemoteLogManager.class);
        when(rlm.isInitialized(t1p0)).thenReturn(false);

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        when(replicaManager.brokerTopicStats()).thenReturn(mock(BrokerTopicStats.class));
        when(replicaManager.localLogOrException(t1p0)).thenReturn(log);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(rlm));

        RemoteLeaderEndPoint remoteLeaderEndPoint = mock(RemoteLeaderEndPoint.class);

        ReplicaFetcherTierStateMachine stateMachine = new ReplicaFetcherTierStateMachine(remoteLeaderEndPoint, replicaManager);
        assertThrows(RemoteStorageException.class,
            () -> stateMachine.buildRemoteLogAuxState(t1p0, 4, 2L, 3, 0L),
            "Remote log metadata is not yet initialized for " + t1p0);
    }

    private LogConfig createLogConfig() {
        Properties properties = new Properties();
        properties.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        return new LogConfig(properties);
    }

    private LeaderEpochFileCache createLeaderEpochCache(TopicPartition tp) {
        LeaderEpochCheckpoint checkpoint = new LeaderEpochCheckpoint() {
            private List<EpochEntry> epochs = Collections.emptyList();

            @Override
            public void write(Collection<EpochEntry> epochs) {
                this.epochs = new ArrayList<>(epochs);
            }

            @Override
            public List<EpochEntry> read() {
                return this.epochs;
            }
        };

        return new LeaderEpochFileCache(tp, checkpoint);
    }
}
