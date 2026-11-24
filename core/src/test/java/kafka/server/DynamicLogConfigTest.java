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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.common.DirectoryEventHandler;
import org.apache.kafka.server.config.DynamicLogConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogManager;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamicLogConfigTest {

    @Test
    public void testDynamicLogLocalRetentionMsConfig() {
        Map<String, String> props = createBrokerConfig();
        props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "2592000000");
        KafkaConfig config = new KafkaConfig(props);
        DynamicLogConfig dynamicLogConfig = new DynamicLogConfig(mock(LogManager.class), mock(DirectoryEventHandler.class));
        config.dynamicConfig().initialize(Option.empty());
        config.dynamicConfig().addBrokerReconfigurable(dynamicLogConfig);

        Properties newProps = new Properties();
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2160000000");
        // update default config
        config.dynamicConfig().validate(newProps, false);
        config.dynamicConfig().updateDefaultConfig(newProps, true);
        assertEquals(2160000000L, config.remoteLogManagerConfig().logLocalRetentionMs());

        // update per broker config
        config.dynamicConfig().validate(newProps, true);
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2150000000");
        config.dynamicConfig().updateBrokerConfig(0, newProps, true);
        assertEquals(2150000000L, config.remoteLogManagerConfig().logLocalRetentionMs());
    }

    @Test
    public void testDynamicLogLocalRetentionSizeConfig() {
        Map<String, String> props = createBrokerConfig();
        props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "4294967296");
        KafkaConfig config = new KafkaConfig(props);
        DynamicLogConfig dynamicLogConfig = new DynamicLogConfig(mock(LogManager.class), mock(DirectoryEventHandler.class));
        config.dynamicConfig().initialize(Option.empty());
        config.dynamicConfig().addBrokerReconfigurable(dynamicLogConfig);

        Properties newProps = new Properties();
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967295");
        // update default config
        config.dynamicConfig().validate(newProps, false);
        config.dynamicConfig().updateDefaultConfig(newProps, true);
        assertEquals(4294967295L, config.remoteLogManagerConfig().logLocalRetentionBytes());

        // update per broker config
        config.dynamicConfig().validate(newProps, true);
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967294");
        config.dynamicConfig().updateBrokerConfig(0, newProps, true);
        assertEquals(4294967294L, config.remoteLogManagerConfig().logLocalRetentionBytes());
    }

    @Test
    public void testDynamicLogLocalRetentionSkipsOnInvalidConfig() {
        Map<String, String> props = createBrokerConfig();
        props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "1000");
        props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "1024");
        KafkaConfig config = new KafkaConfig(props);
        config.dynamicConfig().initialize(Option.empty());

        // Check for invalid localRetentionMs < -2
        verifyConfigUpdateWithInvalidConfig(
            config,
            props,
            Map.of(),
            Map.of(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "-3")
        );
        // Check for invalid localRetentionBytes < -2
        verifyConfigUpdateWithInvalidConfig(
            config,
            props,
            Map.of(),
            Map.of(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "-3")
        );
    }

    @Test
    public void testDynamicLogLocalRetentionThrowsOnIncorrectConfig() {
        // Check for incorrect case of logLocalRetentionMs > retentionMs
        verifyIncorrectLogLocalRetentionProps(2000L, 1000L, 2, 100);
        // Check for incorrect case of logLocalRetentionBytes > retentionBytes
        verifyIncorrectLogLocalRetentionProps(500L, 1000L, 200, 100);
        // Check for incorrect case of logLocalRetentionMs (-1 viz unlimited) > retentionMs,
        verifyIncorrectLogLocalRetentionProps(-1, 1000L, 200, 100);
        // Check for incorrect case of logLocalRetentionBytes(-1 viz unlimited) > retentionBytes
        verifyIncorrectLogLocalRetentionProps(2000L, 1000L, -1, 100);
    }

    @Test
    public void testDynamicLogRemoteCopyLagConfig() {
        Map<String, String> props = createBrokerConfig();
        KafkaConfig config = new KafkaConfig(props);
        DynamicLogConfig dynamicLogConfig = new DynamicLogConfig(mock(LogManager.class), mock(DirectoryEventHandler.class));
        config.dynamicConfig().initialize(Option.empty());
        config.dynamicConfig().addBrokerReconfigurable(dynamicLogConfig);
        assertEquals(RemoteLogManagerConfig.DEFAULT_LOG_REMOTE_COPY_LAG_MS, config.remoteLogManagerConfig().logRemoteCopyLagMs());
        assertEquals(RemoteLogManagerConfig.DEFAULT_LOG_REMOTE_COPY_LAG_BYTES, config.remoteLogManagerConfig().logRemoteCopyLagBytes());

        // update default config
        Properties newProps = new Properties();
        newProps.put(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_MS_PROP, "100");
        newProps.put(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_BYTES_PROP, "200");
        config.dynamicConfig().validate(newProps, false);
        config.dynamicConfig().updateDefaultConfig(newProps, true);
        assertEquals(100L, config.remoteLogManagerConfig().logRemoteCopyLagMs());
        assertEquals(200L, config.remoteLogManagerConfig().logRemoteCopyLagBytes());

        // update per broker config
        config.dynamicConfig().validate(newProps, true);
        newProps.put(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_BYTES_PROP, "300");
        config.dynamicConfig().updateBrokerConfig(0, newProps, true);
        assertEquals(300L, config.remoteLogManagerConfig().logRemoteCopyLagBytes());
    }

    @Test
    public void testDynamicLogRemoteCopyLagThrowsOnIncorrectConfig() {
        // log remote copy lag ms cannot exceed effective log local retention ms
        verifyIncorrectLogRemoteCopyLagProps(
            1000L,
            -2L,
            1001L,
            1000L,
            -2L,
            100L
        );

        // log remote copy lag bytes cannot exceed effective log local retention bytes
        verifyIncorrectLogRemoteCopyLagProps(
            1000L,
            -2L,
            100L,
            1000L,
            -2L,
            1001L
        );
    }

    @Test
    public void testDynamicLogConfigHandlesSynonymsCorrectly() {
        Map<String, String> origProps = createBrokerConfig();
        origProps.put(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "1");
        DynamicLogConfigContext ctx = new DynamicLogConfigContext(origProps);
        assertEquals(TimeUnit.MINUTES.toMillis(1), ctx.config.logRetentionTimeMillis());

        Properties props = new Properties();
        props.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, "12345678");
        ctx.config.dynamicConfig().updateDefaultConfig(props, true);
        assertEquals(TimeUnit.MINUTES.toMillis(1), ctx.currentDefaultLogConfig.get().retentionMs);
    }

    @Test
    public void testDynamicLogConfigCordonedLogDirs() {
        Map<String, String> origProps = createBrokerConfig(2);
        DynamicLogConfigContext ctx = new DynamicLogConfigContext(origProps);
        assertTrue(ctx.config.cordonedLogDirs().isEmpty());
        List<String> logDirs = ctx.config.logDirs();
        verify(ctx.directoryEventHandler, never()).handleCordoned(anySet());

        // Cordoning 1 new log dir, so 1 new handleCordoned invocation
        Properties props = new Properties();
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, logDirs.get(0));
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(List.of(logDirs.get(0)), ctx.config.cordonedLogDirs());
        verify(ctx.directoryEventHandler, times(1)).handleCordoned(anySet());

        // When using *, no other entries must be specified, so no new invocations
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, "*,/invalid/log/dir");
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(List.of(logDirs.get(0)), ctx.config.cordonedLogDirs());
        verify(ctx.directoryEventHandler, times(1)).handleCordoned(anySet());

        // Invalid log dir, so no new invocations
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, "/invalid/log/dir");
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(List.of(logDirs.get(0)), ctx.config.cordonedLogDirs());
        verify(ctx.directoryEventHandler, times(1)).handleCordoned(anySet());

        // * cordons the 2nd log dir, so 1 new handleCordoned invocation
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, "*");
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(logDirs, ctx.config.cordonedLogDirs());
        verify(ctx.directoryEventHandler, times(2)).handleCordoned(anySet());

        // same value so no new invocations
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, "*");
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(logDirs, ctx.config.cordonedLogDirs());
        verify(ctx.directoryEventHandler, times(2)).handleCordoned(anySet());

        // clearing all cordoned log dirs, so 1 new handleCordoned invocation
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, "");
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertTrue(ctx.config.cordonedLogDirs().isEmpty());
        verify(ctx.directoryEventHandler, times(3)).handleCordoned(anySet());

        // * cordons all log dirs, so 1 new handleCordoned invocation
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, String.join(",", logDirs));
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(logDirs, ctx.config.cordonedLogDirs());
        verify(ctx.directoryEventHandler, times(4)).handleCordoned(anySet());
    }

    @Test
    public void testDynamicLogConfigRelativeCordonedLogDir() {
        Map<String, String> origProps = createBrokerConfig(2);
        DynamicLogConfigContext ctx = new DynamicLogConfigContext(origProps);
        String relativeLogDir = ctx.config.logDirs().stream()
            .filter(dir -> !new File(dir).isAbsolute())
            .findFirst()
            .orElseThrow();
        String absoluteLogDir = new File(relativeLogDir).getAbsolutePath();
        Uuid directoryId = Uuid.randomUuid();
        // Only the absolute path resolves to an ID, matching how LogManager keys its directory ID map.
        when(ctx.logManagerMock.directoryId(anyString())).thenAnswer(invocation ->
            absoluteLogDir.equals(invocation.getArgument(0)) ? Optional.of(directoryId) : Optional.empty()
        );

        Properties props = new Properties();
        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, relativeLogDir);
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(List.of(relativeLogDir), ctx.config.cordonedLogDirs());
        verify(ctx.logManagerMock).updateCordonedLogDirs(Set.of(absoluteLogDir));
        verify(ctx.directoryEventHandler).handleCordoned(Set.of(directoryId));

        props.put(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, absoluteLogDir);
        ctx.config.dynamicConfig().updateBrokerConfig(0, props, true);
        assertEquals(List.of(absoluteLogDir), ctx.config.cordonedLogDirs());
        verify(ctx.logManagerMock, times(2)).updateCordonedLogDirs(Set.of(absoluteLogDir));
        verify(ctx.directoryEventHandler, times(2)).handleCordoned(Set.of(directoryId));
    }

    @Test
    public void testLogRetentionTimeMinutesIsNotDynamicallyReconfigurable() {
        Map<String, String> origProps = createBrokerConfig();
        origProps.put(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "1");
        DynamicLogConfigContext ctx = new DynamicLogConfigContext(origProps);
        assertEquals(TimeUnit.HOURS.toMillis(1), ctx.config.logRetentionTimeMillis());

        Properties props = new Properties();
        props.put(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "3");
        ctx.config.dynamicConfig().updateDefaultConfig(props, true);
        assertEquals(TimeUnit.HOURS.toMillis(1), ctx.config.logRetentionTimeMillis());
        assertFalse(ctx.currentDefaultLogConfig.get().originals().containsKey(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG));
    }

    @Test
    public void testAdvertisedListenersIsNotDynamicallyReconfigurable() {
        Map<String, String> origProps = createBrokerConfig();
        DynamicLogConfigContext ctx = new DynamicLogConfigContext(origProps);

        // update advertised listeners should not work
        Properties props = new Properties();
        props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "SASL_PLAINTEXT://localhost:8181");
        ctx.config.dynamicConfig().updateDefaultConfig(props, true);
        CollectionConverters.asJava(ctx.config.effectiveAdvertisedBrokerListeners())
            .forEach(endpoint -> assertEquals(SecurityProtocol.PLAINTEXT.name, endpoint.listener()));
        assertFalse(ctx.currentDefaultLogConfig.get().originals().containsKey(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG));
    }

    private static void verifyConfigUpdateWithInvalidConfig(
        KafkaConfig config,
        Map<String, String> origProps,
        Map<String, String> validProps,
        Map<String, String> invalidProps
    ) {
        Properties props = new Properties();
        props.putAll(validProps);
        props.putAll(invalidProps);

        // DynamicBrokerConfig#validate is used by AdminClient to validate the configs provided
        // in an AlterConfigs request. Validation should fail with an exception if any of the configs are invalid.
        assertThrows(ConfigException.class, () -> config.dynamicConfig().validate(props, true));

        // DynamicBrokerConfig#updateBrokerConfig is used to update configs from broker during
        // startup and when configs are updated in broker. Update should apply valid configs and ignore
        // invalid ones.
        config.dynamicConfig().updateBrokerConfig(0, props, true);
        validProps.forEach((name, value) -> assertEquals(value, config.originals().get(name)));
        invalidProps.keySet().forEach(name -> assertEquals(origProps.get(name), config.originals().get(name)));
    }

    private static void verifyIncorrectLogRemoteCopyLagProps(
        long retentionMs,
        long logLocalRetentionMs,
        long logRemoteCopyLagMs,
        long retentionBytes,
        long logLocalRetentionBytes,
        long logRemoteCopyLagBytes
    ) {
        Map<String, String> props = createBrokerConfig();
        props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, Long.toString(retentionMs));
        props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, Long.toString(retentionBytes));
        KafkaConfig config = new KafkaConfig(props);
        DynamicLogConfig dynamicLogConfig = new DynamicLogConfig(mock(LogManager.class), mock(DirectoryEventHandler.class));
        config.dynamicConfig().initialize(Option.empty());
        config.dynamicConfig().addBrokerReconfigurable(dynamicLogConfig);

        Properties newProps = new Properties();
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, Long.toString(logLocalRetentionMs));
        newProps.put(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_MS_PROP, Long.toString(logRemoteCopyLagMs));
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, Long.toString(logLocalRetentionBytes));
        newProps.put(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_BYTES_PROP, Long.toString(logRemoteCopyLagBytes));
        // validate default config
        assertThrows(ConfigException.class, () -> config.dynamicConfig().validate(newProps, false));
        // validate per broker config
        assertThrows(ConfigException.class, () -> config.dynamicConfig().validate(newProps, true));
    }

    private static void verifyIncorrectLogLocalRetentionProps(
        long logLocalRetentionMs,
        long retentionMs,
        long logLocalRetentionBytes,
        long retentionBytes
    ) {
        Map<String, String> props = createBrokerConfig();
        props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, Long.toString(retentionMs));
        props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, Long.toString(retentionBytes));
        KafkaConfig config = new KafkaConfig(props);
        DynamicLogConfig dynamicLogConfig = new DynamicLogConfig(mock(LogManager.class), mock(DirectoryEventHandler.class));
        config.dynamicConfig().initialize(Option.empty());
        config.dynamicConfig().addBrokerReconfigurable(dynamicLogConfig);

        Properties newProps = new Properties();
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, Long.toString(logLocalRetentionMs));
        newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, Long.toString(logLocalRetentionBytes));
        // validate default config
        assertThrows(ConfigException.class, () -> config.dynamicConfig().validate(newProps, false));
        // validate per broker config
        assertThrows(ConfigException.class, () -> config.dynamicConfig().validate(newProps, true));
    }

    private static Map<String, String> createBrokerConfig() {
        return createBrokerConfig(1);
    }

    private static Map<String, String> createBrokerConfig(int logDirCount) {
        // Alternate absolute and relative log directories, starting with an absolute path.
        String logDirs = IntStream.range(0, logDirCount)
            .mapToObj(i -> i % 2 == 0
                ? TestUtils.tempDirectory().getAbsolutePath()
                : TestUtils.tempRelativeDir("data").getPath())
            .collect(Collectors.joining(","));

        return new HashMap<>(Map.of(
            KRaftConfigs.NODE_ID_CONFIG, "0",
            KRaftConfigs.PROCESS_ROLES_CONFIG, "broker",
            KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER",
            QuorumConfig.QUORUM_VOTERS_CONFIG, "1000@localhost:0",
            SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:8181",
            SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:8181",
            SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            ServerLogConfigs.LOG_DIRS_CONFIG, logDirs
        ));
    }

    private static class DynamicLogConfigContext {
        private final KafkaConfig config;
        private final LogManager logManagerMock;
        private final DirectoryEventHandler directoryEventHandler;
        private final AtomicReference<LogConfig> currentDefaultLogConfig;

        private DynamicLogConfigContext(Map<String, String> origProps) {
            config = new KafkaConfig(origProps);
            logManagerMock = mock(LogManager.class);
            directoryEventHandler = mock(DirectoryEventHandler.class);

            when(logManagerMock.allLogs()).thenReturn(Set.of());
            when(logManagerMock.directoryId(anyString())).thenAnswer(ignored -> Optional.of(Uuid.randomUuid()));

            currentDefaultLogConfig = new AtomicReference<>(new LogConfig(new Properties()));
            when(logManagerMock.currentDefaultConfig()).thenAnswer(ignored -> currentDefaultLogConfig.get());
            doAnswer(invocation -> {
                currentDefaultLogConfig.set(invocation.getArgument(0));
                return null;
            }).when(logManagerMock).reconfigureDefaultLogConfig(any(LogConfig.class));

            config.dynamicConfig().initialize(Option.empty());
            config.dynamicConfig().addBrokerReconfigurable(new DynamicLogConfig(logManagerMock, directoryEventHandler));
        }
    }
}
