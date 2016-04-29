/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSourceConnector;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ReflectionsUtil;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
    protected final StatusBackingStore statusBackingStore;
    protected final ConfigBackingStore configBackingStore;

    private Map<String, Connector> tempConnectors = new ConcurrentHashMap<>();
    private static List<ConnectorPluginInfo> validConnectorPlugins;
    private static final Object LOCK = new Object();
    private Thread classPathTraverser;
    private static final List<Class<? extends Connector>> EXCLUDES = Arrays.<Class<? extends Connector>>asList(VerifiableSourceConnector.class, VerifiableSinkConnector.class);

    public AbstractHerder(Worker worker,
                          String workerId,
                          StatusBackingStore statusBackingStore,
                          ConfigBackingStore configBackingStore) {
        this.worker = worker;
        this.workerId = workerId;
        this.statusBackingStore = statusBackingStore;
        this.configBackingStore = configBackingStore;
    }

    protected abstract int generation();

    protected void startServices() {
        this.worker.start();
        this.statusBackingStore.start();
        this.configBackingStore.start();
        traverseClassPath();
    }

    protected void stopServices() {
        this.statusBackingStore.stop();
        this.configBackingStore.stop();
        this.worker.stop();
        if (this.classPathTraverser != null) {
            try {
                this.classPathTraverser.join();
            } catch (InterruptedException e) {
                // ignore as it can only happen during shutdown
            }
        }
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
            statusBackingStore.put(new TaskStatus(status.id(), TaskStatus.State.DESTROYED, workerId, generation()));
        statusBackingStore.put(new ConnectorStatus(connector, ConnectorStatus.State.DESTROYED, workerId, generation()));
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

        return new ConnectorStateInfo(connName, connectorState, taskStates);
    }

    @Override
    public ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id) {
        TaskStatus status = statusBackingStore.get(id);

        if (status == null)
            throw new NotFoundException("No status found for task " + id);

        return new ConnectorStateInfo.TaskState(id.task(), status.state().toString(),
                status.workerId(), status.trace());
    }


    @Override
    public ConfigInfos validateConfigs(String connType, Map<String, String> connectorConfig) {
        ConfigDef connectorConfigDef = ConnectorConfig.configDef();
        List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(connectorConfig);
        ConfigInfos result = generateResult(connType, connectorConfigDef.configKeys(), connectorConfigValues, Collections.<String>emptyList());

        if (result.errorCount() != 0) {
            return result;
        }

        Connector connector = getConnector(connType);

        Config config = connector.validate(connectorConfig);
        ConfigDef configDef = connector.config();
        Map<String, ConfigKey> configKeys = configDef.configKeys();
        List<ConfigValue> configValues = config.configValues();

        Map<String, ConfigKey> resultConfigKeys = new HashMap<>(configKeys);
        resultConfigKeys.putAll(connectorConfigDef.configKeys());
        configValues.addAll(connectorConfigValues);

        List<String> allGroups = new LinkedList<>(connectorConfigDef.groups());
        List<String> groups = configDef.groups();
        allGroups.addAll(groups);

        return generateResult(connType, resultConfigKeys, configValues, allGroups);
    }

    public static List<ConnectorPluginInfo> connectorPlugins() {
        synchronized (LOCK) {
            if (validConnectorPlugins != null) {
                return validConnectorPlugins;
            }
            ReflectionsUtil.registerUrlTypes();
            ConfigurationBuilder builder = new ConfigurationBuilder().setUrls(ClasspathHelper.forJavaClassPath());
            Reflections reflections = new Reflections(builder);

            Set<Class<? extends Connector>> connectorClasses = reflections.getSubTypesOf(Connector.class);
            connectorClasses.removeAll(EXCLUDES);
            List<ConnectorPluginInfo> connectorPlugins = new LinkedList<>();
            for (Class<? extends Connector> connectorClass : connectorClasses) {
                int mod = connectorClass.getModifiers();
                if (!Modifier.isAbstract(mod) && !Modifier.isInterface(mod)) {
                    connectorPlugins.add(new ConnectorPluginInfo(connectorClass.getCanonicalName()));
                }
            }
            validConnectorPlugins = connectorPlugins;
            return connectorPlugins;
        }
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
                configValue.addErrorMessage("Configuration is not defined: " + configName);
                configInfoList.add(new ConfigInfo(null, convertConfigValue(configValue, null)));
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

    private static ConfigKeyInfo convertConfigKey(ConfigKey configKey) {
        String name = configKey.name;
        Type type = configKey.type;
        String typeName = configKey.type.name();

        boolean required = false;
        String defaultValue;
        if (configKey.defaultValue == ConfigDef.NO_DEFAULT_VALUE) {
            defaultValue = (String) configKey.defaultValue;
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

    private Connector getConnector(String connType) {
        if (tempConnectors.containsKey(connType)) {
            return tempConnectors.get(connType);
        } else {
            Connector connector = worker.getConnector(connType);
            tempConnectors.put(connType, connector);
            return connector;
        }
    }

    private String trace(Throwable t) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        t.printStackTrace(new PrintStream(output));
        try {
            return output.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    private void traverseClassPath() {
        classPathTraverser = new Thread(new Runnable() {
            @Override
            public void run() {
                connectorPlugins();
            }
        }, "CLASSPATH traversal thread.");
        classPathTraverser.start();
    }
}
