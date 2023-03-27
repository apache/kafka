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

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.util.EnumConverter;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;

public class AclCommand {
    private static String wildcardHost = "*";

    static String authorizerDeprecationMessage = "Warning: support for ACL configuration directly " +
        "through the authorizer is deprecated and will be removed in a future release. Please use " +
        "--bootstrap-server instead to set ACLs through the admin client.";
    static ResourcePatternFilter clusterResourceFilter = new ResourcePatternFilter(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);

    private final static String newline = scala.util.Properties.lineSeparator();
    public static void main(String... args) {
        AclCommandOptions opts = new AclCommandOptions(args);
        CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to manage acls on kafka.");
        opts.checkArgs();
        AclCommandService aclCommandService;

    }

    interface AclCommandService {
        public void addAcls();
        public void removeAcls();
        public void listAcls();
    }
    static class AdminClientService implements AclCommandService {
        private final AclCommandOptions opts;

        AdminClientService(AclCommandOptions opts) {
            this.opts = opts;
        }

        private void withAdminClient(AclCommandOptions opts, Consumer<Admin> fn) {
            Properties props = new Properties();
            if (opts.options.has(opts.commandConfigOpt)) {
                try {
                    props = Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
            Admin adminClient = Admin.create(props);

            try {
                fn.accept(adminClient);
            } finally {
                adminClient.close();
            }
        }

        @Override
        public void addAcls() {
            Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcl = getResourceToAcls(opts);
            withAdminClient(opts, admin -> {
                resourceToAcl.forEach((resource, acls) -> {
                    System.out.println("Adding ACLs for resource " + resource + ":\n" +
                        acls.stream().map(acl -> "\t" + acl).collect(Collectors.joining("\n")) + "\n");
                    Collection<AclBinding> aclBindings = acls.stream()
                        .map(acl -> new AclBinding(resource, acl))
                        .collect(Collectors.toList());
                    try {
                        admin.createAcls(aclBindings).all().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });
                listAcls(admin);
            });
        }

        @Override
        public void removeAcls() {
            withAdminClient(opts, admin -> {
                Map<ResourcePatternFilter, Set<AccessControlEntry>> filterToAcl = getResourceFilterToAcls(opts);
                filterToAcl.forEach((filter, acls) -> {
                    if (acls.isEmpty()) {
                        if (confirmAction(opts, String.format("Are you sure you want to delete all ACLs for resource filter `%s`? (y/n)", filter))) {
                            removeAcls(admin, acls, filter);
                        }
                    } else {
                        if (confirmAction(opts, String.format("Are you sure you want to remove ACLs:\n%s\nfrom resource filter `%s`? (y/n)",
                            acls.stream().map(acl -> "\t" + acl).collect(Collectors.joining("\n")), filter))) {
                            removeAcls(admin, acls, filter);
                        }
                    }
                });
            });

        }

        @Override
        public void listAcls() {
            withAdminClient(opts, admin -> listAcls(admin));
        }

        private void listAcls(Admin adminClient) {
            Set<ResourcePatternFilter> filters = getResourceFilter(opts, false);
            Set<KafkaPrincipal> listPrincipals = getPrincipals(opts, opts.listPrincipalsOpt);
            Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls = getAcls(adminClient, filters);

            if (listPrincipals.isEmpty()) {
                printResourceAcls(resourceToAcls);
            } else {
                for (KafkaPrincipal principal : listPrincipals) {
                    System.out.println("ACLs for principal `" + principal + "`");
                    Map<ResourcePattern, Set<AccessControlEntry>> filteredResourceToAcls = resourceToAcls.entrySet().stream()
                        .map(entry -> {
                            ResourcePattern resource = entry.getKey();
                            Set<AccessControlEntry> acls = entry.getValue().stream()
                                .filter(acl -> principal.toString().equals(acl.principal()))
                                .collect(Collectors.toSet());
                            return new AbstractMap.SimpleEntry<>(resource, acls);
                        })
                        .filter(entry -> !entry.getValue().isEmpty())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    printResourceAcls(filteredResourceToAcls);
                }
            }
        }

        private static void printResourceAcls(Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls) {
            for (Map.Entry<ResourcePattern, Set<AccessControlEntry>> entry : resourceToAcls.entrySet()) {
                ResourcePattern resource = entry.getKey();
                Set<AccessControlEntry> acls = entry.getValue();
                System.out.println("Current ACLs for resource " + resource + ":\n" +
                    acls.stream().map(acl -> "\t" + acl).collect(Collectors.joining("\n")) + "\n");
            }
        }

        private static void removeAcls(Admin adminClient, Set<AccessControlEntry> acls, ResourcePatternFilter filter) {
            try {
                if (acls.isEmpty()) {
                    adminClient.deleteAcls(Collections.singletonList(new AclBindingFilter(filter, AccessControlEntryFilter.ANY))).all().get();
                } else {
                    List<AclBindingFilter> aclBindingFilters = acls.stream().map(acl -> new AclBindingFilter(filter, acl.toFilter())).collect(Collectors.toList());
                    adminClient.deleteAcls(aclBindingFilters).all().get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class AuthorizerService implements AclCommandService {
        private static final Logger log = LoggerFactory.getLogger(AuthorizerService.class);

        private final String authorizerClassName;
        private final AclCommandOptions opts;

        AuthorizerService(String authorizerClassName, AclCommandOptions opts) {
            this.authorizerClassName = authorizerClassName;
            this.opts = opts;
        }

        @Override
        public void addAcls() {
            Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcl = getResourceToAcls(opts);
            withAuthorizer(authorizer -> {
                resourceToAcl.entrySet().forEach(entry -> {
                    ResourcePattern resource = entry.getKey();
                    Set<AccessControlEntry> acls = entry.getValue();
                    System.out.println("Adding ACLs for resource `" + resource + "`: \n" +
                            acls.stream().map(acl -> "\t" + acl).collect(Collectors.joining("\n")) + "\n");
                    List<AclBinding> aclBindings = acls.stream().map(acl -> new AclBinding(resource, acl)).collect(Collectors.toList());
                    authorizer.createAcls(null, aclBindings).forEach(result -> {
                        try {
                            AclCreateResult aclCreateResult = result.toCompletableFuture().get();
                            Optional.ofNullable(aclCreateResult.exception()).ifPresent(exception -> {
                                System.out.println("Error while adding ACLs: " + exception.get().getMessage());
                                System.out.println(Utils.stackTrace(exception.get()));
                            });
                        } catch (ExecutionException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
                });
                listAcls();
            });
        }

        @Override
        public void removeAcls() {
            withAuthorizer(authorizer -> {
                Map<ResourcePatternFilter, Set<AccessControlEntry>> filterToAcl = getResourceFilterToAcls(opts);
                filterToAcl.entrySet().stream().forEach(resourcePatternFilterSetEntry -> {
                    ResourcePatternFilter filter = resourcePatternFilterSetEntry.getKey();
                    Set<AccessControlEntry> acls = resourcePatternFilterSetEntry.getValue();

                    if (acls.isEmpty()) {
                        if (confirmAction(opts, String.format("Are you sure you want to delete all ACLs for resource filter `%s`? (y/n)", filter))) {
                            removeAcls(authorizer, acls, filter);
                        }
                    } else {
                        if (confirmAction(opts, String.format("Are you sure you want to remove ACLs: %s %s %s from resource filter `%s`? (y/n)", newline,
                                acls.stream().map(acl -> "\t" + acl)
                                .collect(Collectors.joining(newline)), newline, filter))) {
                            removeAcls(authorizer, acls, filter);
                        }
                    }
                });
                listAcls();
            });
        }

        @Override
        public void listAcls() {
            withAuthorizer(authorizer -> {
                Set<ResourcePatternFilter> filters = getResourceFilter(opts, false);
                Set<KafkaPrincipal> listPrincipals = getPrincipals(opts, opts.listPrincipalsOpt);
                Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls = getAcls(authorizer, filters);

                if (listPrincipals.isEmpty()) {
                    resourceToAcls.forEach((resource, acls) -> System.out.println(
                        String.format("Current ACLs for resource `%s`: %s %s %s", resource, newline,
                            acls.stream().map(acl -> "\t" + acl).collect(Collectors.joining(newline)), newline)));
                } else {
                    listPrincipals.forEach(principal -> {
                        System.out.println(String.format("ACLs for principal `%s`", principal));
                        Map<ResourcePattern, Set<AccessControlEntry>> filteredResourceToAcls =
                            resourceToAcls.entrySet().stream().map(entry -> {
                                ResourcePattern resource = entry.getKey();
                                Set<AccessControlEntry> acls = entry.getValue();
                                Set<AccessControlEntry> accessControlEntries = acls.stream()
                                    .filter(acl -> principal.equals(acl.principal()))
                                    .collect(Collectors.toSet());
                                return new AbstractMap.SimpleEntry<>(resource, accessControlEntries);
                            })
                                .filter(entry -> !entry.getValue().isEmpty())
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        filteredResourceToAcls.forEach((resource, acls) -> {
                            System.out.println("Current ACLs for resource `" + resource + "`:\n" +
                                acls.stream().map(acl -> "\t" + acl).collect(Collectors.joining("\n")) + "\n");
                        });
                    });
                }
            });
        }

        private void withAuthorizer(Consumer<Authorizer> fn) {
            // It is possible that zookeeper.set.acl could be true without SASL if mutual certificate authentication is configured.
            // We will default the value of zookeeper.set.acl to true or false based on whether SASL is configured,
            // but if SASL is not configured and zookeeper.set.acl is supposed to be true due to mutual certificate authentication
            // then it will be up to the user to explicitly specify zookeeper.set.acl=true in the authorizer-properties.
            Map<String, Boolean> defaultProps = new HashMap<>();
            defaultProps.put(kafka.server.KafkaConfig$.MODULE$.ZkEnableSecureAclsProp(), JaasUtils.isZkSaslEnabled());
            Map<String, Object> authorizerPropertiesWithoutTls = new HashMap<>(defaultProps);
            if (opts.options.has(opts.authorizerPropertiesOpt)) {
                List<String> authorizerProperties = opts.options.valuesOf(opts.authorizerPropertiesOpt);
                Properties properties = CommandLineUtils.parseKeyValueArgs(authorizerProperties, false);
                for (final String name: properties.stringPropertyNames())
                    authorizerPropertiesWithoutTls.put(name, properties.getProperty(name));
            }
            Map<String, Object> authorizerProperties = authorizerPropertiesWithoutTls;
            if (opts.options.has(opts.zkTlsConfigFile)) {
                List<String> validKeys = scala.collection.JavaConverters.asJava(kafka.server.KafkaConfig$.MODULE$.ZkSslConfigToSystemPropertyMap().keySet().toSeq());
                List<String> zkSslConfigToSystemProperty =
                        scala.collection.JavaConverters.asJava(kafka.server.KafkaConfig$.MODULE$.ZkSslConfigToSystemPropertyMap().keySet()).stream()
                                .map(key -> "authorizer." + key)
                                .collect(Collectors.toList());
                validKeys.addAll(zkSslConfigToSystemProperty);
                try {
                    Properties zkTlsProps = Utils.loadProps(opts.options.valueOf(opts.zkTlsConfigFile), validKeys);
                    for (final String name: zkTlsProps.stringPropertyNames())
                        authorizerProperties.put(name, zkTlsProps.getProperty(name));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            Authorizer authZ = kafka.security.authorizer.AuthorizerUtils.createAuthorizer(authorizerClassName);
            try {
                authZ.configure(new HashMap<>(authorizerProperties));
                fn.accept(authZ);
            } finally {
                try {
                    authZ.close();
                } catch (IOException e) {
                    log.warn(e.getMessage(), e);
                }
            }

        }

        private void removeAcls(Authorizer authorizer, Set<AccessControlEntry> acls, ResourcePatternFilter filter) {
            Collection<AclBindingFilter> aclBindingFilters = acls.isEmpty() ?
                Collections.singletonList(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)) :
                acls.stream().map(acl -> new AclBindingFilter(filter, acl.toFilter())).collect(Collectors.toList());
            List<? extends CompletionStage<AclDeleteResult>> result = authorizer.deleteAcls(null, new ArrayList<>(aclBindingFilters));
            result.forEach(deleteResult -> {
                try {
                    AclDeleteResult completeResult = deleteResult.toCompletableFuture().get();
                    Exception exception = completeResult.exception().orElseThrow(() -> new IllegalStateException("Unexpected error while removing ACLs"));
                    System.out.println("Error while removing ACLs: " + exception.getMessage());
                    System.out.println(Utils.stackTrace(exception));

                    completeResult.aclBindingDeleteResults().forEach(aclBindingDeleteResult -> {
                        Exception aclBindingException = aclBindingDeleteResult.exception().orElseThrow(() -> new IllegalStateException("Unexpected error while removing ACLs"));
                        System.out.println("Error while removing ACLs: " + aclBindingException.getMessage());
                        System.out.println(Utils.stackTrace(aclBindingException));
                    });
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        Map<ResourcePattern, Set<AccessControlEntry>> getAcls(Authorizer authorizer, Set<ResourcePatternFilter> filters) {
            List<AclBinding> aclBindings = filters.isEmpty() ?
                StreamSupport.stream(authorizer.acls(AclBindingFilter.ANY).spliterator(), false).collect(Collectors.toList()) :
                filters.stream().flatMap(filter -> StreamSupport.stream(
                    authorizer.acls(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)).spliterator(), false)
                ).collect(Collectors.toList());

            Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls = new HashMap<>();

            for (AclBinding aclBinding : aclBindings) {
                ResourcePattern pattern = aclBinding.pattern();
                Set<AccessControlEntry> acls = resourceToAcls.computeIfAbsent(pattern, p -> new HashSet<>());
                acls.add(aclBinding.entry());
            }

            return resourceToAcls;
        }

    }

    private static Map<ResourcePattern, Set<AccessControlEntry>> getResourceToAcls(AclCommandOptions opts) {
        PatternType patternType = opts.options.valueOf(opts.resourcePatternType);
        if (!patternType.isSpecific()) {
            CommandLineUtils.printUsageAndExit(opts.parser, "A '--resource-pattern-type' value of '" + patternType + "' is not valid when adding acls.");
        }

        Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcl = getResourceFilterToAcls(opts).entrySet().stream()
            .collect(Collectors.toMap(e -> new ResourcePattern(e.getKey().resourceType(), e.getKey().name(), e.getKey().patternType()), Map.Entry::getValue));

        if (resourceToAcl.values().stream().anyMatch(acl -> acl.isEmpty())) {
            CommandLineUtils.printUsageAndExit(opts.parser, "You must specify one of: --allow-principal, --deny-principal when trying to add ACLs.");
        }

        return resourceToAcl;
    }

    private static Map<ResourcePatternFilter, Set<AccessControlEntry>> getResourceFilterToAcls(AclCommandOptions opts) {
        Map<ResourcePatternFilter, Set<AccessControlEntry>> resourceToAcls = new HashMap<>();

        //if none of the --producer or --consumer options are specified , just construct ACLs from CLI options.
        if (!opts.options.has(opts.producerOpt) && !opts.options.has(opts.consumerOpt)) {
            resourceToAcls.putAll(getCliResourceFilterToAcls(opts));
        }

        //users are allowed to specify both --producer and --consumer options in a single command.
        if (opts.options.has(opts.producerOpt)) {
            resourceToAcls.putAll(getProducerResourceFilterToAcls(opts));
        }

        if (opts.options.has(opts.consumerOpt)) {
            getConsumerResourceFilterToAcls(opts).forEach((k, v) -> {
                Set<AccessControlEntry> existingAcls = resourceToAcls.getOrDefault(k, new HashSet<>());
                existingAcls.addAll(v);
                resourceToAcls.put(k, existingAcls);
            });
        }

        validateOperation(opts, resourceToAcls);

        return resourceToAcls;
    }

    private static Map<ResourcePatternFilter, Set<AccessControlEntry>> getConsumerResourceFilterToAcls(AclCommandOptions opts) {
        Set<ResourcePatternFilter> filters = getResourceFilter(opts, true);

        Set<ResourcePatternFilter> topics = filters.stream()
            .filter(filter -> filter.resourceType().equals(ResourceType.TOPIC))
            .collect(Collectors.toSet());
        Set<ResourcePatternFilter> groups = filters.stream()
            .filter(filter -> filter.resourceType().equals(ResourceType.GROUP))
            .collect(Collectors.toSet());

        //Read, Describe on topic, Read on consumerGroup
        Set<AccessControlEntry> acls = getAcl(opts, new HashSet<>(Arrays.asList(AclOperation.READ, AclOperation.DESCRIBE)));

        Map<ResourcePatternFilter, Set<AccessControlEntry>> resourceFilterToAcls = new HashMap<>();
        topics.forEach(topic -> resourceFilterToAcls.put(topic, acls));
        groups.forEach(group -> resourceFilterToAcls.put(group, getAcl(opts, Collections.singleton(AclOperation.READ))));

        return resourceFilterToAcls;
    }

    private static Map<ResourcePatternFilter, Set<AccessControlEntry>> getCliResourceFilterToAcls(AclCommandOptions opts) {
        Set<AccessControlEntry> acls = getAcl(opts);
        Set<ResourcePatternFilter> filters = getResourceFilter(opts, true);
        return filters.stream().collect(Collectors.toMap(filter -> filter, filter -> acls));
    }

    private static Map<ResourcePatternFilter, Set<AccessControlEntry>> getProducerResourceFilterToAcls(AclCommandOptions opts) {
        Set<ResourcePatternFilter> filters = getResourceFilter(opts, true);
        Set<ResourcePatternFilter> topics = filters.stream().filter(x -> x.resourceType() == ResourceType.TOPIC).collect(Collectors.toSet());
        Set<ResourcePatternFilter> transactionalIds = filters.stream().filter(x -> x.resourceType() == ResourceType.TRANSACTIONAL_ID).collect(Collectors.toSet());
        boolean enableIdempotence = opts.options.has(opts.idempotentOpt);

        Set<AccessControlEntry> topicAcls = getAcl(opts, new HashSet<>(Arrays.asList(WRITE, DESCRIBE, CREATE)));
        Set<AccessControlEntry> transactionalIdAcls = getAcl(opts, new HashSet<>(Arrays.asList(WRITE, DESCRIBE)));

        Map<ResourcePatternFilter, Set<AccessControlEntry>> result = new HashMap<>();

        for (ResourcePatternFilter topic : topics) {
            result.put(topic, topicAcls);
        }

        for (ResourcePatternFilter transactionalId : transactionalIds) {
            result.put(transactionalId, transactionalIdAcls);
        }

        if (enableIdempotence) {
            result.put(clusterResourceFilter, getAcl(opts, Collections.singleton(IDEMPOTENT_WRITE)));
        }

        return result;
    }

    private static Set<AccessControlEntry> getAcl(AclCommandOptions opts, Set<AclOperation> operations) {
        Set<KafkaPrincipal> allowedPrincipals = getPrincipals(opts, opts.allowPrincipalsOpt);

        Set<KafkaPrincipal> deniedPrincipals = getPrincipals(opts, opts.denyPrincipalsOpt);

        Set<String> allowedHosts = getHosts(opts, opts.allowHostsOpt, opts.allowPrincipalsOpt);

        Set<String> deniedHosts = getHosts(opts, opts.denyHostsOpt, opts.denyPrincipalsOpt);

        Set<AccessControlEntry> acls = new HashSet<>();
        if (!allowedHosts.isEmpty() && !allowedPrincipals.isEmpty()) {
            acls.addAll(getAcls(allowedPrincipals, ALLOW, operations, allowedHosts));
        }

        if (!deniedHosts.isEmpty() && !deniedPrincipals.isEmpty()) {
            acls.addAll(getAcls(deniedPrincipals, DENY, operations, deniedHosts));
        }

        return acls;
    }

    private static Set<String> getHosts(AclCommandOptions opts, OptionSpec<String> hostOptionSpec,
                                        OptionSpec<String> principalOptionSpec) {
        Set<String> hosts = new HashSet<>();

        if (opts.options.has(hostOptionSpec)) {
            hosts.addAll(opts.options.valuesOf(hostOptionSpec).stream().map(s -> s.trim()).collect(Collectors.toSet()));
        } else if (opts.options.has(principalOptionSpec)) {
            hosts.add(wildcardHost);
        }

        return hosts;
    }

    private static Set<KafkaPrincipal> getPrincipals(AclCommandOptions opts, OptionSpec<String> principalOptionSpec) {
        Set<KafkaPrincipal> principals = Collections.emptySet();
        if (opts.options.has(principalOptionSpec)) {
            principals = new HashSet<>();
            List<String> principalStrings = opts.options.valuesOf(principalOptionSpec);
            for (String principalString : principalStrings) {
                principals.add(SecurityUtils.parseKafkaPrincipal(principalString.trim()));
            }
        }
        return principals;
    }
    private static Set<AccessControlEntry> getAcl(AclCommandOptions opts) {
        Set<AclOperation> operations = opts.options.valuesOf(opts.operationsOpt).stream()
            .map(operation -> SecurityUtils.operation(operation.trim()))
            .collect(Collectors.toSet());
        return getAcl(opts, operations);
    }

    private static Map<ResourcePattern, Set<AccessControlEntry>> getAcls(Admin adminClient, Set<ResourcePatternFilter> filters) {
        List<AclBinding> aclBindings;
        try {
            if (filters.isEmpty()) {
                aclBindings = new ArrayList<>(adminClient.describeAcls(AclBindingFilter.ANY).values().get());
            } else {
                List<List<AclBinding>> results = new ArrayList<>();
                for (ResourcePatternFilter filter : filters) {
                    results.add(new ArrayList<>(adminClient.describeAcls(new AclBindingFilter(filter, AccessControlEntryFilter.ANY))
                        .values().get()));
                }
                aclBindings = results.stream().flatMap(List::stream).collect(Collectors.toList());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls = new HashMap<>();
        for (AclBinding aclBinding : aclBindings) {
            ResourcePattern resource = aclBinding.pattern();
            Set<AccessControlEntry> acls = resourceToAcls.getOrDefault(resource, new HashSet<>());
            acls.add(aclBinding.entry());
            resourceToAcls.put(resource, acls);
        }
        return resourceToAcls;
    }

    public static Set<AccessControlEntry> getAcls(Set<KafkaPrincipal> principals,
                                                  AclPermissionType permissionType,
                                                  Set<AclOperation> operations,
                                                  Set<String> hosts) {
        Set<AccessControlEntry> acls = new HashSet<>();
        for (KafkaPrincipal principal : principals) {
            for (AclOperation operation : operations) {
                for (String host : hosts) {
                    acls.add(new AccessControlEntry(principal.toString(), host, operation, permissionType));
                }
            }
        }
        return acls;
    }

    private static Set<ResourcePatternFilter> getResourceFilter(AclCommandOptions opts, boolean dieIfNoResourceFound) {
        PatternType patternType = opts.options.valueOf(opts.resourcePatternType);
        Set<ResourcePatternFilter> resourceFilters = new HashSet<>();

        if (opts.options.has(opts.topicOpt)) {
            opts.options.valuesOf(opts.topicOpt).forEach(topic ->
                resourceFilters.add(new ResourcePatternFilter(ResourceType.TOPIC, topic.trim(), patternType)));
        }

        if (patternType == PatternType.LITERAL && (opts.options.has(opts.clusterOpt) || opts.options.has(opts.idempotentOpt))) {
            resourceFilters.add(clusterResourceFilter);
        }

        if (opts.options.has(opts.groupOpt)) {
            opts.options.valuesOf(opts.groupOpt).forEach(group ->
                resourceFilters.add(new ResourcePatternFilter(ResourceType.GROUP, group.trim(), patternType)));
        }

        if (opts.options.has(opts.transactionalIdOpt)) {
            opts.options.valuesOf(opts.transactionalIdOpt).forEach(transactionalId ->
                resourceFilters.add(new ResourcePatternFilter(ResourceType.TRANSACTIONAL_ID, transactionalId, patternType)));
        }

        if (opts.options.has(opts.delegationTokenOpt)) {
            opts.options.valuesOf(opts.delegationTokenOpt).forEach(token ->
                resourceFilters.add(new ResourcePatternFilter(ResourceType.DELEGATION_TOKEN, token.trim(), patternType)));
        }

        if (opts.options.has(opts.userPrincipalOpt)) {
            opts.options.valuesOf(opts.userPrincipalOpt).forEach(user ->
                resourceFilters.add(new ResourcePatternFilter(ResourceType.USER, user.trim(), patternType)));
        }

        if (resourceFilters.isEmpty() && dieIfNoResourceFound) {
            CommandLineUtils.printUsageAndExit(opts.parser, "You must provide at least one resource: --topic <topic> or --cluster or --group <group> or --delegation-token <Delegation Token ID>");
        }

        return resourceFilters;
    }

    private static Boolean confirmAction(AclCommandOptions opts, String msg) {
        if (opts.options.has(opts.forceOpt))
            return true;
        return System.console().readLine(msg).equalsIgnoreCase("y");
    }

    private static void validateOperation(AclCommandOptions opts, Map<ResourcePatternFilter, Set<AccessControlEntry>> resourceToAcls) {
        for (Map.Entry<ResourcePatternFilter, Set<AccessControlEntry>> entry : resourceToAcls.entrySet()) {
            ResourcePatternFilter resource = entry.getKey();
            Set<AccessControlEntry> acls = entry.getValue();
            Collection<AclOperation> validOps = scala.collection.JavaConverters.asJavaCollection(kafka.security.authorizer.AclEntry.supportedOperations(resource.resourceType()));
            validOps.add(AclOperation.ALL);
            Set<AclOperation> unsupportedOps = new HashSet<>();
            for (AccessControlEntry acl : acls) {
                if (!validOps.contains(acl.operation())) {
                    unsupportedOps.add(acl.operation());
                }
            }
            if (!unsupportedOps.isEmpty()) {
                String msg = String.format("ResourceType %s only supports operations %s", resource.resourceType(), validOps);
                CommandLineUtils.printUsageAndExit(opts.parser, msg);
            }
        }
    }

    static class AclCommandOptions extends CommandDefaultOptions {
        private final String commandConfigDoc = "A property file containing configs to be passed to Admin Client.";
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        ArgumentAcceptingOptionSpec<String> authorizerOpt;
        ArgumentAcceptingOptionSpec<String> authorizerPropertiesOpt;
        ArgumentAcceptingOptionSpec<String> topicOpt;
        OptionSpecBuilder clusterOpt;
        ArgumentAcceptingOptionSpec<String> groupOpt;
        ArgumentAcceptingOptionSpec<String> transactionalIdOpt;
        OptionSpecBuilder idempotentOpt;
        ArgumentAcceptingOptionSpec<String> delegationTokenOpt;
        ArgumentAcceptingOptionSpec<PatternType> resourcePatternType;
        OptionSpecBuilder addOpt;
        OptionSpecBuilder removeOpt;
        OptionSpecBuilder listOpt;
        ArgumentAcceptingOptionSpec<String> operationsOpt;
        ArgumentAcceptingOptionSpec<String> allowPrincipalsOpt;
        ArgumentAcceptingOptionSpec<String> denyPrincipalsOpt;
        ArgumentAcceptingOptionSpec<String> listPrincipalsOpt;
        ArgumentAcceptingOptionSpec<String> allowHostsOpt;
        ArgumentAcceptingOptionSpec<String> denyHostsOpt;
        OptionSpecBuilder producerOpt;
        OptionSpecBuilder forceOpt;
        OptionSpecBuilder consumerOpt;
        ArgumentAcceptingOptionSpec<String> zkTlsConfigFile;
        ArgumentAcceptingOptionSpec<String> userPrincipalOpt;


        public AclCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "A list of host/port pairs to use for establishing the connection to the Kafka cluster." +
                    " This list should be in the form host1:port1,host2:port2,... This config is required for acl management using admin client API.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);

            commandConfigOpt = parser.accepts("command-config", commandConfigDoc)
                .withOptionalArg()
                .describedAs("command-config")
                .ofType(String.class);

            authorizerOpt = parser.accepts("authorizer", "DEPRECATED: Fully qualified class name of " +
                    "the authorizer, which defaults to kafka.security.authorizer.AclAuthorizer if --bootstrap-server is not provided. " +
                    AclCommand.authorizerDeprecationMessage)
                .withRequiredArg()
                .describedAs("authorizer")
                .ofType(String.class);

            authorizerPropertiesOpt = parser.accepts("authorizer-properties", "DEPRECATED: The " +
                    "properties required to configure an instance of the Authorizer specified by --authorizer. " +
                    "These are key=val pairs. For the default authorizer, example values are: zookeeper.connect=localhost:2181. " +
                    AclCommand.authorizerDeprecationMessage)
                .withRequiredArg()
                .describedAs("authorizer-properties")
                .ofType(String.class);

            topicOpt = parser.accepts("topic", "topic to which ACLs should be added or removed. " +
                    "A value of '*' indicates ACL should apply to all topics.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);

            clusterOpt = parser.accepts("cluster", "Add/Remove cluster ACLs.");
            groupOpt = parser.accepts("group", "Consumer Group to which the ACLs should be added or removed. " +
                    "A value of '*' indicates the ACLs should apply to all groups.")
                .withRequiredArg()
                .describedAs("group")
                .ofType(String.class);

            transactionalIdOpt = parser.accepts("transactional-id", "The transactionalId to which ACLs should " +
                    "be added or removed. A value of '*' indicates the ACLs should apply to all transactionalIds.")
                .withRequiredArg()
                .describedAs("transactional-id")
                .ofType(String.class);

            idempotentOpt = parser.accepts("idempotent", "Enable idempotence for the producer. This should be " +
                "used in combination with the --producer option. Note that idempotence is enabled automatically if " +
                "the producer is authorized to a particular transactional-id.");

            delegationTokenOpt = parser.accepts("delegation-token", "Delegation token to which ACLs should be added or removed. " +
                    "A value of '*' indicates ACL should apply to all tokens.")
                .withRequiredArg()
                .describedAs("delegation-token")
                .ofType(String.class);

            resourcePatternType = parser.accepts("resource-pattern-type", "The type of the resource pattern or pattern filter. " +
                    "When adding acls, this should be a specific pattern type, e.g. 'literal' or 'prefixed'. " +
                    "When listing or removing acls, a specific pattern type can be used to list or remove acls from specific resource patterns, " +
                    "or use the filter values of 'any' or 'match', where 'any' will match any pattern type, but will match the resource name exactly, " +
                    "where as 'match' will perform pattern matching to list or remove all acls that affect the supplied resource(s). " +
                    "WARNING: 'match', when used in combination with the '--remove' switch, should be used with care.")
                .withRequiredArg()
                .ofType(String.class)
                .withValuesConvertedBy(new PatternTypeConverter(PatternType.class))
                .defaultsTo(PatternType.LITERAL);

            addOpt = parser.accepts("add", "Indicates you are trying to add ACLs.");
            removeOpt = parser.accepts("remove", "Indicates you are trying to remove ACLs.");
            listOpt = parser.accepts("list", "List ACLs for the specified resource, use --topic <topic> or --group <group> or --cluster to specify a resource.");

            operationsOpt = parser.accepts("operation", "Operation that is being allowed or denied. Valid operation names are: " + newline +
                            kafka.security.authorizer.AclEntry.AclOperations().map(operation -> "\t" + SecurityUtils.operationName(operation)).mkString(newline) + newline)
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo(SecurityUtils.operationName(AclOperation.ALL));

            allowPrincipalsOpt = parser.accepts("allow-principal", "principal is in principalType:name format." +
                    " Note that principalType must be supported by the Authorizer being used." +
                    " For example, User:'*' is the wild card indicating all users.")
                .withRequiredArg()
                .describedAs("allow-principal")
                .ofType(String.class);

            denyPrincipalsOpt = parser.accepts("deny-principal", "principal is in principalType:name format. " +
                    "By default anyone not added through --allow-principal is denied access. " +
                    "You only need to use this option as negation to already allowed set. " +
                    "Note that principalType must be supported by the Authorizer being used. " +
                    "For example if you wanted to allow access to all users in the system but not test-user you can define an ACL that " +
                    "allows access to User:'*' and specify --deny-principal=User:test@EXAMPLE.COM. " +
                    "AND PLEASE REMEMBER DENY RULES TAKES PRECEDENCE OVER ALLOW RULES.")
                .withRequiredArg()
                .describedAs("deny-principal")
                .ofType(String.class);

            listPrincipalsOpt = parser.accepts("principal", "List ACLs for the specified principal. principal is in principalType:name format." +
                    " Note that principalType must be supported by the Authorizer being used. Multiple --principal option can be passed.")
                .withOptionalArg()
                .describedAs("principal")
                .ofType(String.class);

            allowHostsOpt = parser.accepts("allow-host", "Host from which principals listed in --allow-principal will have access. " +
                    "If you have specified --allow-principal then the default for this option will be set to '*' which allows access from all hosts.")
                .withRequiredArg()
                .describedAs("allow-host")
                .ofType(String.class);

            denyHostsOpt = parser.accepts("deny-host", "Host from which principals listed in --deny-principal will be denied access. " +
                    "If you have specified --deny-principal then the default for this option will be set to '*' which denies access from all hosts.")
                .withRequiredArg()
                .describedAs("deny-host")
                .ofType(String.class);

            producerOpt = parser.accepts("producer", "Convenience option to add/remove ACLs for producer role. " +
                "This will generate ACLs that allows WRITE,DESCRIBE and CREATE on topic.");

            consumerOpt = parser.accepts("consumer", "Convenience option to add/remove ACLs for consumer role. " +
                "This will generate ACLs that allows READ,DESCRIBE on topic and READ on group.");

            forceOpt = parser.accepts("force", "Assume Yes to all queries and do not prompt.");

            List<String> zkSslConfigKeys = new ArrayList<>(scala.collection.JavaConverters.asJavaCollection(kafka.server.KafkaConfig$.MODULE$.ZkSslConfigToSystemPropertyMap().keys()));
            zkTlsConfigFile = parser.accepts("zk-tls-config-file",
                    "DEPRECATED: Identifies the file where ZooKeeper client TLS connectivity properties are defined for" +
                        " the default authorizer kafka.security.authorizer.AclAuthorizer." +
                        " Any properties other than the following (with or without an \"authorizer.\" prefix) are ignored: " +
                        zkSslConfigKeys.stream().sorted().collect(Collectors.joining(", ")) +
                        ". Note that if SASL is not configured and zookeeper.set.acl is supposed to be true due to mutual certificate authentication being used" +
                        " then it is necessary to explicitly specify --authorizer-properties zookeeper.set.acl=true. " +
                        AclCommand.authorizerDeprecationMessage)
                .withRequiredArg().describedAs("Authorizer ZooKeeper TLS configuration").ofType(String.class);

            userPrincipalOpt = parser.accepts("user-principal", "Specifies a user principal as a resource in relation with the operation. For instance " +
                    "one could grant CreateTokens or DescribeTokens permission on a given user principal.")
                .withRequiredArg()
                .describedAs("user-principal")
                .ofType(String.class);

            options = parser.parse(args);

        }

        public void checkArgs() {
            validArgsWithBootstrapServerOpt();

            long actions = Arrays.asList(addOpt, removeOpt, listOpt).stream().filter(opt -> options.has(opt)).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --list, --add, --remove. ");

            CommandLineUtils.checkInvalidArgs(parser, options, listOpt, producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostsOpt, denyPrincipalsOpt);

            //when --producer or --consumer is specified , user should not specify operations as they are inferred and we also disallow --deny-principals and --deny-hosts.
            CommandLineUtils.checkInvalidArgs(parser, options, producerOpt, operationsOpt, denyPrincipalsOpt, denyHostsOpt);
            CommandLineUtils.checkInvalidArgs(parser, options, consumerOpt, operationsOpt, denyPrincipalsOpt, denyHostsOpt);

            if (options.has(listPrincipalsOpt) && !options.has(listOpt))
                CommandLineUtils.printUsageAndExit(parser, "The --principal option is only available if --list is set");

            if (options.has(producerOpt) && !options.has(topicOpt))
                CommandLineUtils.printUsageAndExit(parser, "With --producer you must specify a --topic");

            if (options.has(idempotentOpt) && !options.has(producerOpt))
                CommandLineUtils.printUsageAndExit(parser, "The --idempotent option is only available if --producer is set");

            if (options.has(consumerOpt) && (!options.has(topicOpt) || !options.has(groupOpt) || (!options.has(producerOpt) && (options.has(clusterOpt) || options.has(transactionalIdOpt)))))
                CommandLineUtils.printUsageAndExit(parser, "With --consumer you must specify a --topic and a --group and no --cluster or --transactional-id option should be specified.");
        }

        private void validArgsWithBootstrapServerOpt() {
            if (options.has(bootstrapServerOpt) && options.has(authorizerOpt))
                CommandLineUtils.printUsageAndExit(parser, "Only one of --bootstrap-server or --authorizer must be specified");

            if (!options.has(bootstrapServerOpt)) {
                CommandLineUtils.checkRequiredArgs(parser, options, authorizerPropertiesOpt);
                System.err.println(AclCommand.authorizerDeprecationMessage);
            }

            if (options.has(commandConfigOpt) && !options.has(bootstrapServerOpt))
                CommandLineUtils.printUsageAndExit(parser, "The --command-config option can only be used with --bootstrap-server option");

            if (options.has(authorizerPropertiesOpt) && options.has(bootstrapServerOpt))
                CommandLineUtils.printUsageAndExit(parser, "The --authorizer-properties option can only be used with --authorizer option");
        }
    }

    static class PatternTypeConverter extends EnumConverter<PatternType> {

        protected PatternTypeConverter(Class<PatternType> clazz) {
            super(clazz);
        }

        @Override
        public PatternType convert(String value) {
            PatternType patternType = super.convert(value);
            if (patternType.isUnknown()) {
                throw new IllegalArgumentException("Unknown resource-pattern-type: " + value);
            }
            return patternType;
        }

        @Override
        public String valuePattern() {
            List<PatternType> values = Arrays.asList(PatternType.values());
            List<PatternType> filteredValues = values.stream()
                .filter(type -> type != PatternType.UNKNOWN)
                .collect(Collectors.toList());
            return String.join("|", filteredValues.stream()
                .map(Object::toString)
                .collect(Collectors.toList()));
        }
    }

}
