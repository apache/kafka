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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Abstract Herder implementation which handles connector/task lifecycle tracking. Extensions
 * must invoke the lifecycle hooks appropriately.
 *
 * This class takes the following approach for sending status updates to the backing store:
 *
 * 1) When the connector or task is starting, we overwrite the previous state blindly. This ensures that
 *    every rebalance will reset the state of tasks to the proper state. The intuition is that there should
 *    be less chance of write conflicts when the worker has just received its assignment and is starting tasks.
 *    In particular, this prevents us from depending on the generation absolutely. If the group disappears
 *    and the generation is reset, then we'll overwrite the status information with the older (and larger)
 *    generation with the updated one. The danger of this approach is that slow starting tasks may cause the
 *    status to be overwritten after a rebalance has completed.
 *
 * 2) If the connector or task fails or is shutdown, we use {@link StatusBackingStore#putSafe(ConnectorStatus)},
 *    which provides a little more protection if the worker is no longer in the group (in which case the
 *    task may have already been started on another worker). Obviously this is still racy. If the task has just
 *    started on another worker, we may not have the updated status cached yet. In this case, we'll overwrite
 *    the value which will cause the state to be inconsistent (most likely until the next rebalance). Until
 *    we have proper producer groups with fenced groups, there is not much else we can do.
 */
public abstract class AbstractHerder implements Herder, TaskStatus.Listener, ConnectorStatus.Listener {

    private final String workerId;
    protected final Worker worker;
    private final String kafkaClusterId;
    protected final StatusBackingStore statusBackingStore;
    protected final ConfigBackingStore configBackingStore;
    private final ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy;
    protected volatile boolean running = false;
    private final ExecutorService connectorExecutor;

    private final ConcurrentMap<String, Connector> tempConnectors = new ConcurrentHashMap<>();

    public AbstractHerder(Worker worker,
                          String workerId,
                          String kafkaClusterId,
                          StatusBackingStore statusBackingStore,
                          ConfigBackingStore configBackingStore,
                          ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        this.worker = worker;
        this.worker.herder = this;
        this.workerId = workerId;
        this.kafkaClusterId = kafkaClusterId;
        this.statusBackingStore = statusBackingStore;
        this.configBackingStore = configBackingStore;
        this.connectorClientConfigOverridePolicy = connectorClientConfigOverridePolicy;
        this.connectorExecutor = Executors.newCachedThreadPool();
    }

    @Override
    public String kafkaClusterId() {
        return kafkaClusterId;
    }

    protected abstract int generation();

    protected void startServices() {
        this.worker.start();
        this.statusBackingStore.start();
        this.configBackingStore.start();
    }

    protected void stopServices() {
        this.statusBackingStore.stop();
        this.configBackingStore.stop();
        this.worker.stop();
        this.connectorExecutor.shutdown();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void onStartup(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.RUNNING,
                workerId, generation()));
    }

    @Override
    public void onPause(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.PAUSED,
                workerId, generation()));
    }

    @Override
    public void onResume(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, TaskStatus.State.RUNNING,
                workerId, generation()));
    }

    @Override
    public void onShutdown(String connector) {
        statusBackingStore.putSafe(new ConnectorStatus(connector, ConnectorStatus.State.UNASSIGNED,
                workerId, generation()));
    }

    @Override
    public void onFailure(String connector, Throwable cause) {
        statusBackingStore.putSafe(new ConnectorStatus(connector, ConnectorStatus.State.FAILED,
                trace(cause), workerId, generation()));
    }

    @Override
    public void onStartup(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, generation()));
    }

    @Override
    public void onFailure(ConnectorTaskId id, Throwable cause) {
        statusBackingStore.putSafe(new TaskStatus(id, TaskStatus.State.FAILED, workerId, generation(), trace(cause)));
    }

    @Override
    public void onShutdown(ConnectorTaskId id) {
        statusBackingStore.putSafe(new TaskStatus(id, TaskStatus.State.UNASSIGNED, workerId, generation()));
    }

    @Override
    public void onResume(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, generation()));
    }

    @Override
    public void onPause(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.PAUSED, workerId, generation()));
    }

    @Override
    public void onDeletion(String connector) {
        for (TaskStatus status : statusBackingStore.getAll(connector))
            onDeletion(status.id());
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.DESTROYED, workerId, generation()));
    }

    @Override
    public void onDeletion(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.DESTROYED, workerId, generation()));
    }

    public void onRestart(String connector) {
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.RESTARTING,
                workerId, generation()));
    }

    public void onRestart(ConnectorTaskId id) {
        statusBackingStore.put(new TaskStatus(id, TaskStatus.State.RESTARTING, workerId, generation()));
    }

    @Override
    public void pauseConnector(String connector) {
        if (!configBackingStore.contains(connector))
            throw new NotFoundException("Unknown connector " + connector);
        configBackingStore.putTargetState(connector, TargetState.PAUSED);
    }

    @Override
    public void resumeConnector(String connector) {
        if (!configBackingStore.contains(connector))
            throw new NotFoundException("Unknown connector " + connector);
        configBackingStore.putTargetState(connector, TargetState.STARTED);
    }

    @Override
    public Plugins plugins() {
        return worker.getPlugins();
    }

    /*
     * Retrieves raw config map by connector name.
     */
    protected abstract Map<String, String> rawConfig(String connName);

    @Override
    public void connectorConfig(String connName, Callback<Map<String, String>> callback) {
        // Subset of connectorInfo, so piggy back on that implementation
        connectorInfo(connName, (error, result) -> {
            if (error != null)
                callback.onCompletion(error, null);
            else
                callback.onCompletion(null, result.config());
        });
    }

    @Override
    public Collection<String> connectors() {
        return configBackingStore.snapshot().connectors();
    }

    @Override
    public ConnectorInfo connectorInfo(String connector) {
        final ClusterConfigState configState = configBackingStore.snapshot();

        if (!configState.contains(connector))
            return null;
        Map<String, String> config = configState.rawConnectorConfig(connector);

        return new ConnectorInfo(
            connector,
            config,
            configState.tasks(connector),
            connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG))
        );
    }

    protected Map<ConnectorTaskId, Map<String, String>> buildTasksConfig(String connector) {
        final ClusterConfigState configState = configBackingStore.snapshot();

        if (!configState.contains(connector))
            return Collections.emptyMap();

        Map<ConnectorTaskId, Map<String, String>> configs = new HashMap<>();
        for (ConnectorTaskId cti : configState.tasks(connector)) {
            configs.put(cti, configState.taskConfig(cti));
        }

        return configs;
    }

    @Override
    public ConnectorStateInfo connectorStatus(String connName) {
        ConnectorStatus connector = statusBackingStore.get(connName);
        if (connector == null)
            throw new NotFoundException("No status found for connector " + connName);
        
        Collection<TaskStatus> tasks = statusBackingStore.getAll(connName);

        ConnectorStateInfo.ConnectorState connectorState = new ConnectorStateInfo.ConnectorState(
                connector.state().toString(), connector.workerId(), connector.trace());
        List<ConnectorStateInfo.TaskState> taskStates = new ArrayList<>();

        for (TaskStatus status : tasks) {
            taskStates.add(new ConnectorStateInfo.TaskState(status.id().task(),
                    status.state().toString(), status.workerId(), status.trace()));
        }

        Collections.sort(taskStates);

        Map<String, String> conf = rawConfig(connName);
        return new ConnectorStateInfo(connName, connectorState, taskStates,
            conf == null ? ConnectorType.UNKNOWN : connectorTypeForClass(conf.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)));
    }

    @Override
    public ActiveTopicsInfo connectorActiveTopics(String connName) {
        Collection<String> topics = statusBackingStore.getAllTopics(connName).stream()
                .map(TopicStatus::topic)
                .collect(Collectors.toList());
        return new ActiveTopicsInfo(connName, topics);
    }

    @Override
    public void resetConnectorActiveTopics(String connName) {
        statusBackingStore.getAllTopics(connName).stream()
                .forEach(status -> statusBackingStore.deleteTopic(status.connector(), status.topic()));
    }

    @Override
    public StatusBackingStore statusBackingStore() {
        return statusBackingStore;
    }

    @Override
    public ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id) {
        TaskStatus status = statusBackingStore.get(id);

        if (status == null)
            throw new NotFoundException("No status found for task " + id);

        return new ConnectorStateInfo.TaskState(id.task(), status.state().toString(),
                status.workerId(), status.trace());
    }

    protected Map<String, ConfigValue> validateBasicConnectorConfig(Connector connector,
                                                                    ConfigDef configDef,
                                                                    Map<String, String> config) {
        return configDef.validateAll(config);
    }

    @Override
    public void validateConnectorConfig(Map<String, String> connectorProps, Callback<ConfigInfos> callback) {
        validateConnectorConfig(connectorProps, callback, true);
    }

    @Override
    public void validateConnectorConfig(Map<String, String> connectorProps, Callback<ConfigInfos> callback, boolean doLog) {
        connectorExecutor.submit(() -> {
            try {
                ConfigInfos result = validateConnectorConfig(connectorProps, doLog);
                callback.onCompletion(null, result);
            } catch (Throwable t) {
                callback.onCompletion(t, null);
            }
        });
    }

    /**
     * Build the {@link RestartPlan} that describes what should and should not be restarted given the restart request
     * and the current status of the connector and task instances.
     *
     * @param request the restart request; may not be null
     * @return the restart plan, or empty if this worker has no status for the connector named in the request and therefore the
     *         connector cannot be restarted
     */
    public Optional<RestartPlan> buildRestartPlan(RestartRequest request) {
        String connectorName = request.connectorName();
        ConnectorStatus connectorStatus = statusBackingStore.get(connectorName);
        if (connectorStatus == null) {
            return Optional.empty();
        }

        // If requested, mark the connector as restarting
        AbstractStatus.State connectorState = request.shouldRestartConnector(connectorStatus) ? AbstractStatus.State.RESTARTING : connectorStatus.state();
        ConnectorStateInfo.ConnectorState connectorInfoState = new ConnectorStateInfo.ConnectorState(
                connectorState.toString(),
                connectorStatus.workerId(),
                connectorStatus.trace()
        );

        // Collect the task states, If requested, mark the task as restarting
        List<ConnectorStateInfo.TaskState> taskStates = statusBackingStore.getAll(connectorName)
                .stream()
                .map(taskStatus -> {
                    AbstractStatus.State taskState = request.shouldRestartTask(taskStatus) ? AbstractStatus.State.RESTARTING : taskStatus.state();
                    return new ConnectorStateInfo.TaskState(
                            taskStatus.id().task(),
                            taskState.toString(),
                            taskStatus.workerId(),
                            taskStatus.trace()
                    );
                })
                .collect(Collectors.toList());
        // Construct the response from the various states
        Map<String, String> conf = rawConfig(connectorName);
        ConnectorStateInfo stateInfo = new ConnectorStateInfo(
                connectorName,
                connectorInfoState,
                taskStates,
                conf == null ? ConnectorType.UNKNOWN : connectorTypeForClass(conf.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG))
        );
        return Optional.of(new RestartPlan(request, stateInfo));

    }

    ConfigInfos validateConnectorConfig(Map<String, String> connectorProps, boolean doLog) {
        if (worker.configTransformer() != null) {
            connectorProps = worker.configTransformer().transform(connectorProps);
        }
        String connType = connectorProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
        if (connType == null)
            throw new BadRequestException("Connector config " + connectorProps + " contains no connector type");

        Connector connector = getConnector(connType);
        org.apache.kafka.connect.health.ConnectorType connectorType;
        ClassLoader savedLoader = plugins().compareAndSwapLoaders(connector);
        try {
            ConfigDef baseConfigDef;
            if (connector instanceof SourceConnector) {
                baseConfigDef = SourceConnectorConfig.configDef();
                connectorType = org.apache.kafka.connect.health.ConnectorType.SOURCE;
            } else {
                baseConfigDef = SinkConnectorConfig.configDef();
                SinkConnectorConfig.validate(connectorProps);
                connectorType = org.apache.kafka.connect.health.ConnectorType.SINK;
            }
            ConfigDef enrichedConfigDef = ConnectorConfig.enrich(plugins(), baseConfigDef, connectorProps, false);
            Map<String, ConfigValue> validatedConnectorConfig = validateBasicConnectorConfig(
                    connector,
                    enrichedConfigDef,
                    connectorProps
            );
            connectorProps.entrySet().stream()
                .filter(e -> e.getValue() == null)
                .map(Map.Entry::getKey)
                .forEach(prop ->
                    validatedConnectorConfig.computeIfAbsent(prop, ConfigValue::new)
                        .addErrorMessage("Null value can not be supplied as the configuration value.")
            );
            List<ConfigValue> configValues = new ArrayList<>(validatedConnectorConfig.values());
            Map<String, ConfigKey> configKeys = new LinkedHashMap<>(enrichedConfigDef.configKeys());
            Set<String> allGroups = new LinkedHashSet<>(enrichedConfigDef.groups());

            // do custom connector-specific validation
            ConfigDef configDef = connector.config();
            if (null == configDef) {
                throw new BadRequestException(
                        String.format(
                                "%s.config() must return a ConfigDef that is not null.",
                                connector.getClass().getName()
                        )
                );
            }
            Config config = connector.validate(connectorProps);
            if (null == config) {
                throw new BadRequestException(
                        String.format(
                                "%s.validate() must return a Config that is not null.",
                                connector.getClass().getName()
                        )
                );
            }
            configKeys.putAll(configDef.configKeys());
            allGroups.addAll(configDef.groups());
            configValues.addAll(config.configValues());
            ConfigInfos configInfos =  generateResult(connType, configKeys, configValues, new ArrayList<>(allGroups));

            AbstractConfig connectorConfig = new AbstractConfig(new ConfigDef(), connectorProps, doLog);
            String connName = connectorProps.get(ConnectorConfig.NAME_CONFIG);
            ConfigInfos producerConfigInfos = null;
            ConfigInfos consumerConfigInfos = null;
            ConfigInfos adminConfigInfos = null;
            if (connectorType.equals(org.apache.kafka.connect.health.ConnectorType.SOURCE)) {
                producerConfigInfos = validateClientOverrides(connName,
                                                              ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX,
                                                              connectorConfig,
                                                              ProducerConfig.configDef(),
                                                              connector.getClass(),
                                                              connectorType,
                                                              ConnectorClientConfigRequest.ClientType.PRODUCER,
                                                              connectorClientConfigOverridePolicy);
                return mergeConfigInfos(connType, configInfos, producerConfigInfos);
            } else {
                consumerConfigInfos = validateClientOverrides(connName,
                                                              ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX,
                                                              connectorConfig,
                                                              ProducerConfig.configDef(),
                                                              connector.getClass(),
                                                              connectorType,
                                                              ConnectorClientConfigRequest.ClientType.CONSUMER,
                                                              connectorClientConfigOverridePolicy);
                // check if topic for dead letter queue exists
                String topic = connectorProps.get(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG);
                if (topic != null && !topic.isEmpty()) {
                    adminConfigInfos = validateClientOverrides(connName,
                                                               ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX,
                                                               connectorConfig,
                                                               ProducerConfig.configDef(),
                                                               connector.getClass(),
                                                               connectorType,
                                                               ConnectorClientConfigRequest.ClientType.ADMIN,
                                                               connectorClientConfigOverridePolicy);
                }

            }
            return mergeConfigInfos(connType, configInfos, consumerConfigInfos, adminConfigInfos);
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }
    }

    private static ConfigInfos mergeConfigInfos(String connType, ConfigInfos... configInfosList) {
        int errorCount = 0;
        List<ConfigInfo> configInfoList = new LinkedList<>();
        Set<String> groups = new LinkedHashSet<>();
        for (ConfigInfos configInfos : configInfosList) {
            if (configInfos != null) {
                errorCount += configInfos.errorCount();
                configInfoList.addAll(configInfos.values());
                groups.addAll(configInfos.groups());
            }
        }
        return new ConfigInfos(connType, errorCount, new ArrayList<>(groups), configInfoList);
    }

    private static ConfigInfos validateClientOverrides(String connName,
                                                      String prefix,
                                                      AbstractConfig connectorConfig,
                                                      ConfigDef configDef,
                                                      Class<? extends Connector> connectorClass,
                                                      org.apache.kafka.connect.health.ConnectorType connectorType,
                                                      ConnectorClientConfigRequest.ClientType clientType,
                                                      ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        int errorCount = 0;
        List<ConfigInfo> configInfoList = new LinkedList<>();
        Map<String, ConfigKey> configKeys = configDef.configKeys();
        Set<String> groups = new LinkedHashSet<>();
        Map<String, Object> clientConfigs = new HashMap<>();
        for (Map.Entry<String, Object> rawClientConfig : connectorConfig.originalsWithPrefix(prefix).entrySet()) {
            String configName = rawClientConfig.getKey();
            Object rawConfigValue = rawClientConfig.getValue();
            ConfigKey configKey = configDef.configKeys().get(configName);
            Object parsedConfigValue = configKey != null
                ? ConfigDef.parseType(configName, rawConfigValue, configKey.type)
                : rawConfigValue;
            clientConfigs.put(configName, parsedConfigValue);
        }
        ConnectorClientConfigRequest connectorClientConfigRequest = new ConnectorClientConfigRequest(
            connName, connectorType, connectorClass, clientConfigs, clientType);
        List<ConfigValue> configValues = connectorClientConfigOverridePolicy.validate(connectorClientConfigRequest);
        if (configValues != null) {
            for (ConfigValue validatedConfigValue : configValues) {
                ConfigKey configKey = configKeys.get(validatedConfigValue.name());
                ConfigKeyInfo configKeyInfo = null;
                if (configKey != null) {
                    if (configKey.group != null) {
                        groups.add(configKey.group);
                    }
                    configKeyInfo = convertConfigKey(configKey, prefix);
                }

                ConfigValue configValue = new ConfigValue(prefix + validatedConfigValue.name(), validatedConfigValue.value(),
                                                          validatedConfigValue.recommendedValues(), validatedConfigValue.errorMessages());
                if (configValue.errorMessages().size() > 0) {
                    errorCount++;
                }
                ConfigValueInfo configValueInfo = convertConfigValue(configValue, configKey != null ? configKey.type : null);
                configInfoList.add(new ConfigInfo(configKeyInfo, configValueInfo));
            }
        }
        return new ConfigInfos(connectorClass.toString(), errorCount, new ArrayList<>(groups), configInfoList);
    }

    // public for testing
    public static ConfigInfos generateResult(String connType, Map<String, ConfigKey> configKeys, List<ConfigValue> configValues, List<String> groups) {
        int errorCount = 0;
        List<ConfigInfo> configInfoList = new LinkedList<>();

        Map<String, ConfigValue> configValueMap = new HashMap<>();
        for (ConfigValue configValue: configValues) {
            String configName = configValue.name();
            configValueMap.put(configName, configValue);
            if (!configKeys.containsKey(configName)) {
                configInfoList.add(new ConfigInfo(null, convertConfigValue(configValue, null)));
                errorCount += configValue.errorMessages().size();
            }
        }

        for (Map.Entry<String, ConfigKey> entry : configKeys.entrySet()) {
            String configName = entry.getKey();
            ConfigKeyInfo configKeyInfo = convertConfigKey(entry.getValue());
            Type type = entry.getValue().type;
            ConfigValueInfo configValueInfo = null;
            if (configValueMap.containsKey(configName)) {
                ConfigValue configValue = configValueMap.get(configName);
                configValueInfo = convertConfigValue(configValue, type);
                errorCount += configValue.errorMessages().size();
            }
            configInfoList.add(new ConfigInfo(configKeyInfo, configValueInfo));
        }
        return new ConfigInfos(connType, errorCount, groups, configInfoList);
    }

    public static ConfigKeyInfo convertConfigKey(ConfigKey configKey) {
        return convertConfigKey(configKey, "");
    }

    private static ConfigKeyInfo convertConfigKey(ConfigKey configKey, String prefix) {
        String name = prefix + configKey.name;
        Type type = configKey.type;
        String typeName = configKey.type.name();

        boolean required = false;
        String defaultValue;
        if (ConfigDef.NO_DEFAULT_VALUE.equals(configKey.defaultValue)) {
            defaultValue = null;
            required = true;
        } else {
            defaultValue = ConfigDef.convertToString(configKey.defaultValue, type);
        }
        String importance = configKey.importance.name();
        String documentation = configKey.documentation;
        String group = configKey.group;
        int orderInGroup = configKey.orderInGroup;
        String width = configKey.width.name();
        String displayName = configKey.displayName;
        List<String> dependents = configKey.dependents;
        return new ConfigKeyInfo(name, typeName, required, defaultValue, importance, documentation, group, orderInGroup, width, displayName, dependents);
    }

    private static ConfigValueInfo convertConfigValue(ConfigValue configValue, Type type) {
        String value = ConfigDef.convertToString(configValue.value(), type);
        List<String> recommendedValues = new LinkedList<>();

        if (type == Type.LIST) {
            for (Object object: configValue.recommendedValues()) {
                recommendedValues.add(ConfigDef.convertToString(object, Type.STRING));
            }
        } else {
            for (Object object : configValue.recommendedValues()) {
                recommendedValues.add(ConfigDef.convertToString(object, type));
            }
        }
        return new ConfigValueInfo(configValue.name(), value, recommendedValues, configValue.errorMessages(), configValue.visible());
    }

    protected Connector getConnector(String connType) {
        return tempConnectors.computeIfAbsent(connType, k -> plugins().newConnector(k));
    }

    /*
     * Retrieves ConnectorType for the corresponding connector class
     * @param connClass class of the connector
     */
    public ConnectorType connectorTypeForClass(String connClass) {
        return ConnectorType.from(getConnector(connClass).getClass());
    }

    /**
     * Checks a given {@link ConfigInfos} for validation error messages and adds an exception
     * to the given {@link Callback} if any were found.
     *
     * @param configInfos configInfos to read Errors from
     * @param callback callback to add config error exception to
     * @return true if errors were found in the config
     */
    protected final boolean maybeAddConfigErrors(
        ConfigInfos configInfos,
        Callback<Created<ConnectorInfo>> callback
    ) {
        int errors = configInfos.errorCount();
        boolean hasErrors = errors > 0;
        if (hasErrors) {
            StringBuilder messages = new StringBuilder();
            messages.append("Connector configuration is invalid and contains the following ")
                .append(errors).append(" error(s):");
            for (ConfigInfo configInfo : configInfos.values()) {
                for (String msg : configInfo.configValue().errors()) {
                    messages.append('\n').append(msg);
                }
            }
            callback.onCompletion(
                new BadRequestException(
                    messages.append(
                        "\nYou can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`"
                    ).toString()
                ), null
            );
        }
        return hasErrors;
    }

    private String trace(Throwable t) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            t.printStackTrace(new PrintStream(output, false, StandardCharsets.UTF_8.name()));
            return output.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    /*
     * Performs a reverse transformation on a set of task configs, by replacing values with variable references.
     */
    public static List<Map<String, String>> reverseTransform(String connName,
                                                             ClusterConfigState configState,
                                                             List<Map<String, String>> configs) {

        // Find the config keys in the raw connector config that have variable references
        Map<String, String> rawConnConfig = configState.rawConnectorConfig(connName);
        Set<String> connKeysWithVariableValues = keysWithVariableValues(rawConnConfig, ConfigTransformer.DEFAULT_PATTERN);

        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> config : configs) {
            Map<String, String> newConfig = new HashMap<>(config);
            for (String key : connKeysWithVariableValues) {
                if (newConfig.containsKey(key)) {
                    newConfig.put(key, rawConnConfig.get(key));
                }
            }
            result.add(newConfig);
        }
        return result;
    }

    // Visible for testing
    static Set<String> keysWithVariableValues(Map<String, String> rawConfig, Pattern pattern) {
        Set<String> keys = new HashSet<>();
        for (Map.Entry<String, String> config : rawConfig.entrySet()) {
            if (config.getValue() != null) {
                Matcher matcher = pattern.matcher(config.getValue());
                if (matcher.find()) {
                    keys.add(config.getKey());
                }
            }
        }
        return keys;
    }

    @Override
    public List<ConfigKeyInfo> connectorPluginConfig(String pluginName) {
        List<ConfigKeyInfo> results = new ArrayList<>();
        ConfigDef configDefs;
        try {
            Plugins p = plugins();
            Object plugin = p.newPlugin(pluginName);
            PluginType pluginType = PluginType.from(plugin.getClass());
            switch (pluginType) {
                case SINK:
                case SOURCE:
                    configDefs = ((Connector) plugin).config();
                    break;
                case CONVERTER:
                    configDefs = ((Converter) plugin).config();
                    break;
                case HEADER_CONVERTER:
                    configDefs = ((HeaderConverter) plugin).config();
                    break;
                case TRANSFORMATION:
                    configDefs = ((Transformation<?>) plugin).config();
                    break;
                case PREDICATE:
                    configDefs = ((Predicate<?>) plugin).config();
                    break;
                default:
                    throw new BadRequestException("Invalid plugin type " + pluginType + ". Valid types are sink, source, converter, header_converter, transformation, predicate.");
            }
        } catch (ClassNotFoundException cnfe) {
            throw new NotFoundException("Unknown plugin " + pluginName + ".");
        }
        for (ConfigDef.ConfigKey configKey : configDefs.configKeys().values()) {
            results.add(AbstractHerder.convertConfigKey(configKey));
        }
        return results;
    }

}
