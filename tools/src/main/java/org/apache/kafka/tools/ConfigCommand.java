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

package org.apache.kafka.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.QuotaConfigResourceTuple;
import org.apache.kafka.common.requests.Resource;
import org.apache.kafka.common.requests.ResourceType;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * This script can be used to change configs for topics/clients/users/brokers dynamically
 * An entity described or altered by the command may be one of:
 * <ul>
 *     <li> topic: --entity-type topics --entity-name <topic>
 *     <li> client: --entity-type clients --entity-name <client-id>
 *     <li> user: --entity-type users --entity-name <user-principal>
 *     <li> <user, client>: --entity-type users --entity-name <user-principal> --entity-type clients --entity-name <client-id>
 *     <li> broker: --entity-type brokers --entity-name <broker>
 * </ul>
 * --entity-default may be used instead of --entity-name when describing or altering default configuration for users and clients.
 *
 */
public class ConfigCommand {

    private AdminClient client;
    private ConfigCommandOptions opts;

    public static void main(String[] args) {
        new ConfigCommand().run(args);
    }

    void run(String[] args) {
        try {
            doRun(args);
        } catch (IOException | ConfigCommandException | ArgumentParserException e) {
            System.err.println("Unexpected error during initialization: ");
            e.printStackTrace(System.err);
            opts.printHelp();
        }
    }

    void doRun(String[] args) throws ArgumentParserException, ConfigCommandException, IOException {
        opts = new ConfigCommandOptions();
        opts.parseArgs(args);

        Properties props = new Properties();
        if (opts.has(ConfigCommandOptions.CONFIG_PROPERTIES)) {
            try (FileInputStream propsFile = new FileInputStream(opts.getConfigProperties())) {
                props.load(propsFile);
            }
        } else {
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.getBootstrapServers());
        }

        this.client = createAdminClient(props);

        if (opts.has(ConfigCommandOptions.DESCRIBE)) {
            describeAndPrintConfigs();
        } else if (opts.has(ConfigCommandOptions.ALTER)) {
            alterAndPrintResult();
        }
    }

    private Map<?, KafkaFuture<Config>> describeConfigs() throws ConfigCommandException {
        ConfigResource.Type resourceType = getConfigResourceType(opts);
        if (resourceType == ConfigResource.Type.TOPIC) {
            if (opts.getEntityName() == null) {
                try {
                    Map<ConfigResource, KafkaFuture<Config>> configs = new HashMap<>();
                    for (String topic : client.listTopics().names().get()) {
                        configs.putAll(describeSingleConfig(resourceType, topic));
                    }
                    return configs;
                } catch (InterruptedException | ExecutionException e) {
                    throw new ConfigCommandException(e);
                }
            } else {
                return describeSingleConfig(resourceType, opts.getEntityName());
            }
        } else if (resourceType == ConfigResource.Type.BROKER) { // describe configs protocol is interpreted over TOPIC and BROKER types only
            return describeSingleConfig(resourceType, opts.getEntityName());
        } else if (resourceType == ConfigResource.Type.USER || resourceType == ConfigResource.Type.CLIENT) {
            ConfigResource.Type childResourceType = getChildConfigResourceType(opts);
            ConfigResource resource = new ConfigResource(resourceType, opts.getEntityDefault() ? "<default>" : opts.getEntityName());
            if (ConfigResource.Type.UNKNOWN == childResourceType) {
                return describeSingleQuotaConfig(resource);
            } else {
                ConfigResource childResource = new ConfigResource(childResourceType, opts.getChildEntityDefault() ? "<default>" : opts.getChildEntityName());
                return describeSingleQuotaConfig(resource, childResource);
            }
        } else {
            throw new IllegalArgumentException("Illegal type specified for the resource");
        }
    }

    private Map<ConfigResource, KafkaFuture<Config>> describeSingleConfig(ConfigResource.Type type, String name) {
        ConfigResource resource = new ConfigResource(type, name);
        DescribeConfigsResult result = client.describeConfigs(Collections.singletonList(resource));
        return result.values();
    }

    private Map<?, KafkaFuture<Config>> describeSingleQuotaConfig(ConfigResource resource) {
        DescribeQuotasResult result = client.describeQuotas(Collections.singletonMap(
                new QuotaConfigResourceTuple(configResourceToResource(resource)), (Collection<String>) Collections.<String>emptyList()));
        return result.values();
    }

    private Map<?, KafkaFuture<Config>> describeSingleQuotaConfig(ConfigResource resource, ConfigResource childResource) {
        DescribeQuotasResult result = client.describeQuotas(Collections.singletonMap(
                new QuotaConfigResourceTuple(configResourceToResource(resource),
                        configResourceToResource(childResource)), (Collection<String>) Collections.<String>emptyList()));
        return result.values();
    }

    private Resource configResourceToResource(ConfigResource configResource) {
        ResourceType resourceType;
        switch (configResource.type()) {
            case TOPIC:
                resourceType = ResourceType.TOPIC;
                break;
            case BROKER:
                resourceType = ResourceType.BROKER;
                break;
            case USER:
                resourceType = ResourceType.USER;
                break;
            case CLIENT:
                resourceType = ResourceType.CLIENT;
                break;
            default:
                throw new IllegalArgumentException("Unexpected resource type " + configResource.type());
        }
        return new Resource(resourceType, configResource.name());
    }

    private <T> Map<T, Config> waitForDescribedConfigs(Map<?, KafkaFuture<Config>> describe, Class<T> clazz) throws ConfigCommandException {
        Map<T, Config> result = new HashMap<>(describe.size());
        for (Map.Entry<?, KafkaFuture<Config>> entry : describe.entrySet()) {
            try {
                result.put(clazz.cast(entry.getKey()), entry.getValue().get());
            } catch (InterruptedException | ExecutionException e) {
                throw new ConfigCommandException(e);
            }
        }
        return result;
    }

    private void describeAndPrintConfigs() throws ConfigCommandException {
        printConfigs(waitForDescribedConfigs(describeConfigs(), Object.class));
    }

    private void alterAndPrintResult() throws ConfigCommandException {
        try {
            Map<ConfigResource, Config> configs = new HashMap<>();
            // compute the result
            if (opts.has(ConfigCommandOptions.DELETE_CONFIG)) {
                configs = deleteConfigsFromResources();
            } else if (opts.has(ConfigCommandOptions.ADD_CONFIG)) {
                configs = getAddConfigs();
            }
            // alter
            client.alterConfigs(configs).all().get();
            // describe the current state
            System.out.println("Configs have been altered. The current state of the specified resource is shown below.");
            describeAndPrintConfigs();
        } catch (InterruptedException | ExecutionException e) {
            throw new ConfigCommandException(e);
        }
    }

    private Map<ConfigResource, Config> deleteConfigsFromResources() throws ConfigCommandException {
        // list the existing configs
        Map<ConfigResource, Config> configResourceMap = waitForDescribedConfigs(describeConfigs(), ConfigResource.class);
        // create a new map without the defaults and the deleted ones
        Map<ConfigResource, Config> filteredConfigs = new HashMap<>();
        for (Map.Entry<ConfigResource, Config> entry : configResourceMap.entrySet()) {
            Collection<ConfigEntry> entryCollection = new LinkedList<>();
            for (ConfigEntry ce : entry.getValue().entries()) {
                // filter default and deleted values
                if (!ce.isDefault() && !opts.getDeleteConfigs().contains(ce.name())) {
                    entryCollection.add(ce);
                }
            }
            filteredConfigs.put(entry.getKey(), new Config(entryCollection));
        }
        return filteredConfigs;
    }

    private Map<ConfigResource, Config> getAddConfigs() {
        Collection<ConfigEntry> configEntries = new LinkedList<>();
        for (Map.Entry<String, String> config : opts.getAddConfigs().entrySet()) {
            configEntries.add(new ConfigEntry(config.getKey(), config.getValue()));
        }
        ConfigResource resource = new ConfigResource(getConfigResourceType(opts), opts.getEntityName());
        return Collections.singletonMap(resource, new Config(configEntries));
    }

    private <T> void printConfigs(Map<T, Config> configs) throws ConfigCommandException {
        for (Map.Entry<T, Config> config : configs.entrySet()) {
            System.out.println();
            if (config.getKey() instanceof ConfigResource) {
                ConfigResource key = (ConfigResource) config.getKey();
                System.out.format("%115s", "CONFIGS FOR " + key.type() + " " + key.name() + "%n");
            } else if (config.getKey() instanceof QuotaConfigResourceTuple) {
                QuotaConfigResourceTuple key = (QuotaConfigResourceTuple) config.getKey();
                Resource parent = key.quotaConfigResource();
                Resource child = key.childQuotaConfigResource();
                if (child != null && child.type() != ResourceType.UNKNOWN) {
                    System.out.format("%115s", "CONFIGS FOR (USER, CLIENT) (" + parent.name() + ", " + child.name() + ")%n");
                } else {
                    System.out.format("%115s", "CONFIGS FOR " + parent.type() + " " + parent.name() + "%n");
                }
            }
            System.out.println();
            System.out.format("%60s   %-70s%10s%11s%9s%n", "Name", "Value", "Sensitive", "Read-only", "Default");
            for (ConfigEntry entry : config.getValue().entries()) {
                String value = entry.value();
                if (value != null && value.length() > 70) {
                    System.out.format("%60s = %-70s%10s%11s%9s%n", entry.name(), entry.value().substring(0, 70), entry.isSensitive(), entry.isReadOnly(), entry.isDefault());
                    System.out.format("%60s   %-70s%n", "", entry.value().substring(70, entry.value().length()));
                } else {
                    System.out.format("%60s = %-70s%10s%11s%9s%n", entry.name(), entry.value(), entry.isSensitive(), entry.isReadOnly(), entry.isDefault());
                }
            }
        }
    }

    /**
     * <b>Visible for testing only.</b>
     * @return an {@link AdminClient}.
     */
    AdminClient createAdminClient(Properties props) {
        return AdminClient.create(props);
    }

    private ConfigResource.Type getConfigResourceType(ConfigCommandOptions opts) {
        ConfigCommandOptions.EntityTypes entityType = opts.getEntityType();
        return toConfigResourceType(entityType);
    }

    private ConfigResource.Type getChildConfigResourceType(ConfigCommandOptions opts) {
        ConfigCommandOptions.EntityTypes entityType = opts.getChildEntityType();
        return toConfigResourceType(entityType);
    }

    private ConfigResource.Type toConfigResourceType(ConfigCommandOptions.EntityTypes entityType) {
        if (entityType == ConfigCommandOptions.EntityTypes.TOPICS) {
            return ConfigResource.Type.TOPIC;
        } else if (entityType == ConfigCommandOptions.EntityTypes.BROKERS) {
            return ConfigResource.Type.BROKER;
        } else if (entityType == ConfigCommandOptions.EntityTypes.USERS) {
            return ConfigResource.Type.USER;
        } else if (entityType == ConfigCommandOptions.EntityTypes.CLIENTS) {
            return ConfigResource.Type.CLIENT;
        } else {
            return ConfigResource.Type.UNKNOWN;
        }
    }

    static class ConfigCommandException extends Exception {

        ConfigCommandException(Throwable cause) {
            super(cause);
        }
    }

    static class ConfigCommandOptions {

        private ArgumentParser parser;
        private Namespace ns;
        private Map<String, String> configs;

        private static final String ESCAPE_QUOTES_PATTERN = "[(^')(^\")('$)(\"$)]";

        private enum EntityTypes {
            TOPICS("topics"),
            CLIENTS("clients"),
            USERS("users"),
            BROKERS("brokers");

            private String name;

            EntityTypes(String name) {
                this.name = name;
            }

            @Override
            public String toString() {
                return name;
            }
        }

        static final String BOOTSTRAP_SERVERS = "bootstrapServers";
        static final String CONFIG_PROPERTIES = "configProperties";
        static final String DESCRIBE = "describe";
        static final String ALTER = "alter";
        static final String ENTITY_TYPE = "entityType";
        static final String ENTITY_NAME = "entityName";
        static final String ENTITY_DEFAULT = "entityDefault";
        static final String CHILD_ENTITY_TYPE = "childEntityType";
        static final String CHILD_ENTITY_NAME = "childEntityName";
        static final String CHILD_ENTITY_DEFAULT = "childEntityDefault";
        static final String FORCE = "force";
        static final String ADD_CONFIG = "addConfig";
        static final String DELETE_CONFIG = "deleteConfig";

        ConfigCommandOptions() {

            this.parser = ArgumentParsers
                    .newArgumentParser("config-command")
                    .defaultHelp(true)
                    .description("Change configs for topics, clients, users, brokers dynamically.");

            // valid for describe and alter modes
            EntityStoreAction entityStoreAction = new EntityStoreAction();
            EntityDefaultStoreAction defaultStoreAction = new EntityDefaultStoreAction();
            this.parser.addArgument("--entity-type")
                    .action(entityStoreAction)
                    .required(true)
                    .type(Arguments.enumStringType(EntityTypes.class))
                    .dest(ENTITY_TYPE)
                    .help("REQUIRED: the type of entity (topics/clients/users/brokers)");

            MutuallyExclusiveGroup nameOptions = parser
                    .addMutuallyExclusiveGroup()
                    .required(true)
                    .description("You can specify only one in --entity-name and --entity-default");

            nameOptions.addArgument("--entity-name")
                    .action(entityStoreAction)
                    .type(String.class)
                    .dest(ENTITY_NAME)
                    .help("Name of entity (client id/user principal name)");
            nameOptions.addArgument("--entity-default")
                    .action(defaultStoreAction)
                    .dest(ENTITY_DEFAULT)
                    .help("Default entity name for clients/users (applies to corresponding entity type in command line)");

            this.parser.addArgument("--force")
                    .type(String.class)
                    .dest(FORCE)
                    .help("Suppresses console prompts");

            // valid only for alter mode
            this.parser.addArgument("--add-config")
                    .type(String.class)
                    .dest(ADD_CONFIG)
                    .help("Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'.");
            this.parser.addArgument("--delete-config")
                    .type(String.class)
                    .dest(DELETE_CONFIG)
                    .help("Config keys to remove in the following form: 'k1,k2'.");

            MutuallyExclusiveGroup operationOptions = parser
                    .addMutuallyExclusiveGroup()
                    .required(true)
                    .description("You can specify only one in --alter, --describe");
            operationOptions.addArgument("--describe")
                    .action(storeTrue())
                    .dest(DESCRIBE)
                    .help("List configs for the given entity.");
            operationOptions.addArgument("--alter")
                    .action(storeTrue())
                    .dest(ALTER)
                    .help("Alter the configuration for the entity.");

            MutuallyExclusiveGroup serverOptions = parser
                    .addMutuallyExclusiveGroup()
                    .required(true)
                    .description("You can specify only one in --bootstrap-servers, --config.properties");
            serverOptions.addArgument("--bootstrap-servers")
                    .type(String.class)
                    .dest(BOOTSTRAP_SERVERS)
                    .help("REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.");
            serverOptions.addArgument("--config.properties")
                    .type(String.class)
                    .dest(CONFIG_PROPERTIES)
                    .help("REQUIRED: The config properties file for the Admin Client.");
        }

        String getBootstrapServers() {
            return this.ns.getString(BOOTSTRAP_SERVERS);
        }

        String getConfigProperties() {
            return this.ns.getString(CONFIG_PROPERTIES);
        }

        EntityTypes getEntityType() {
            return ns.get(ENTITY_TYPE);
        }

        String getEntityName() {
            return ns.getString(ENTITY_NAME);
        }

        EntityTypes getChildEntityType() {
            return ns.get(CHILD_ENTITY_TYPE);
        }

        String getChildEntityName() {
            return ns.get(CHILD_ENTITY_NAME);
        }

        boolean getChildEntityDefault() {
            Boolean def = ns.getBoolean(CHILD_ENTITY_DEFAULT);
            return def == null ? false : def;
        }

        Map<String, String> getAddConfigs() {
            return Collections.unmodifiableMap(configs);
        }

        Set<String> getDeleteConfigs() {
            return Collections.unmodifiableSet(configs.keySet());
        }

        boolean getEntityDefault() {
            Boolean def = ns.getBoolean(ENTITY_DEFAULT);
            return def == null ? false : def;
        }

        boolean isForced() {
            return ns.getBoolean(FORCE);
        }

        void printHelp() {
            parser.printHelp();
        }

        void parseArgs(String[] args) throws ArgumentParserException {
            // late initialization
            if (ns == null) {
                ns = parser.parseArgs(args);
                configs = parseConfigs();
            }
            //perform extra validation
            validate();
        }

        private Map<String, String> parseConfigs() throws ArgumentParserException {
            Map<String, String> confs;
            if (has(ConfigCommandOptions.DELETE_CONFIG)) {
                String[] configsToDelete = ns.getString(DELETE_CONFIG).replaceAll(ESCAPE_QUOTES_PATTERN, "").split(",");
                confs = new HashMap<>(configsToDelete.length);
                for (String config : configsToDelete) {
                    confs.put(config, null);
                }
            } else if (has(ConfigCommandOptions.ADD_CONFIG)) {
                String[] configsToAdd = ns.getString(ADD_CONFIG).replaceAll(ESCAPE_QUOTES_PATTERN, "").split(",");
                confs = new HashMap<>();
                for (String config : configsToAdd) {
                    String[] configArray = config.trim().split("=");
                    if (configArray.length != 2) {
                        throw new ArgumentParserException("Invalid config specified" + config, parser);
                    }
                    confs.put(configArray[0], configArray[1]);
                }
            } else {
                confs = new HashMap<>();
            }
            return confs;
        }

        private void validate() throws ArgumentParserException {
            // in case of alter having an entity name and an add or delete config is a must
            if (has(ALTER)) {
                if (!(has(ADD_CONFIG) || has(DELETE_CONFIG))) {
                    throw new ArgumentParserException("If --alter specified, you must also specify --add-config or --delete-config", parser);
                }
                if (!has(ENTITY_NAME)) {
                    throw new ArgumentParserException("If --alter specified, you must also specify --entity-name", parser);
                }
            } else if (has(DESCRIBE) && (has(ADD_CONFIG) || has(DELETE_CONFIG))) {
                throw new ArgumentParserException("If --describe specified, --add-config and --delete-config are not valid options", parser);
            }
        }

        private boolean has(String option) {
            if (this.ns.get(option) instanceof Boolean)
                return this.ns.getBoolean(option);
            else
                return this.ns.get(option) != null;
        }

        private void checkRequiredArgs(ArgumentParser parser, List<String> required) throws ArgumentParserException {
            for (String arg: required) {
                if (!has(arg))
                    throw new ArgumentParserException("Missing required argument \"" + arg + "\"", parser);
            }
        }

        private static class EntityStoreAction implements ArgumentAction {

            @Override
            public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
                // handle entity types
                switch (arg.getDest()) {
                    case ENTITY_TYPE:
                        EntityTypes entityType = (EntityTypes) attrs.get(ENTITY_TYPE);
                        if (entityType == null) {
                            attrs.put(arg.getDest(), value);
                        } else {
                            EntityTypes childEntityType = (EntityTypes) attrs.get(CHILD_ENTITY_TYPE);
                            if (childEntityType == null) {
                                if (entityType == EntityTypes.BROKERS || entityType == EntityTypes.TOPICS) {
                                    throw new ArgumentParserException("Child entity type can only be specified for users and clients", parser);
                                } else {
                                    attrs.put(CHILD_ENTITY_TYPE, value);
                                }
                            } else {
                                throw new ArgumentParserException("Child entity has already been specified", parser);
                            }
                        }
                        break;
                    case ENTITY_NAME:
                        String entityName = (String) attrs.get(ENTITY_NAME);
                        if (entityName == null) {
                            attrs.put(arg.getDest(), value);
                        } else {
                            String childEntityName = (String) attrs.get(CHILD_ENTITY_NAME);
                            if (childEntityName == null) {
                                attrs.put(CHILD_ENTITY_NAME, value);
                            } else {
                                throw new ArgumentParserException("Child entity has already been specified", parser);
                            }
                        }
                        break;
                    default:
                        throw new ArgumentParserException("Action used on invalid argument", parser);
                }
            }

            @Override
            public void onAttach(Argument arg) {

            }

            @Override
            public boolean consumeArgument() {
                return true;
            }
        }

        private static class EntityDefaultStoreAction implements ArgumentAction {

            @Override
            public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
                if (!attrs.containsKey(ENTITY_DEFAULT)) {
                    attrs.put(ENTITY_DEFAULT, null);
                }
                if (!attrs.containsKey(CHILD_ENTITY_DEFAULT)) {
                    attrs.put(CHILD_ENTITY_DEFAULT, null);
                }
                Boolean entityDefault = (Boolean) attrs.get(ENTITY_DEFAULT);
                if (entityDefault == null) {
                    attrs.put(arg.getDest(), true);
                } else {
                    Boolean childEntityDefault = (Boolean) attrs.get(CHILD_ENTITY_DEFAULT);
                    if (childEntityDefault == null) {
                        attrs.put(CHILD_ENTITY_DEFAULT, true);
                    } else {
                        throw new ArgumentParserException("Child entity has already been specified", parser);
                    }
                }
            }

            @Override
            public void onAttach(Argument arg) {

            }

            @Override
            public boolean consumeArgument() {
                return false;
            }
        }
    }
}
