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
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.security.authorizer.AclEntry;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import joptsimple.AbstractOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.ValueConversionException;
import joptsimple.util.EnumConverter;

import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;

public class AclCommand {

    private static final ResourcePatternFilter CLUSTER_RESOURCE_FILTER =
            new ResourcePatternFilter(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);
    private static final String NL = System.lineSeparator();

    public static void main(String[] args) {
        AclCommandOptions opts = new AclCommandOptions(args);
        AdminClientService aclCommandService = new AdminClientService(opts);
        try (Admin admin = Admin.create(adminConfigs(opts))) {
            if (opts.options.has(opts.addOpt)) {
                aclCommandService.addAcls(admin);
            } else if (opts.options.has(opts.removeOpt)) {
                aclCommandService.removeAcls(admin);
            } else if (opts.options.has(opts.listOpt)) {
                aclCommandService.listAcls(admin);
            }
        } catch (Throwable e) {
            System.out.println("Error while executing ACL command: " + e.getMessage());
            System.out.println(Utils.stackTrace(e));
            Exit.exit(1);
        }
    }

    private static Properties adminConfigs(AclCommandOptions opts) throws IOException {
        Properties props = new Properties();
        if (opts.options.has(opts.commandConfigOpt)) {
            props = Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt));
        }
        if (opts.options.has(opts.bootstrapServerOpt)) {
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
        } else {
            props.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, opts.options.valueOf(opts.bootstrapControllerOpt));
        }
        return props;
    }

    private static class AdminClientService {

        private final AclCommandOptions opts;

        AdminClientService(AclCommandOptions opts) {
            this.opts = opts;
        }

        void addAcls(Admin admin) throws ExecutionException, InterruptedException {
            Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcl = getResourceToAcls(opts);
            for (Map.Entry<ResourcePattern, Set<AccessControlEntry>> entry : resourceToAcl.entrySet()) {
                ResourcePattern resource = entry.getKey();
                Set<AccessControlEntry> acls = entry.getValue();
                System.out.println("Adding ACLs for resource `" + resource + "`: " + NL + " " + acls.stream().map(a -> "\t" + a).collect(Collectors.joining(NL)) + NL);
                Collection<AclBinding> aclBindings = acls.stream().map(acl -> new AclBinding(resource, acl)).toList();
                admin.createAcls(aclBindings).all().get();
            }
        }

        void removeAcls(Admin admin) throws ExecutionException, InterruptedException {
            Map<ResourcePatternFilter, Set<AccessControlEntry>> filterToAcl = getResourceFilterToAcls(opts);
            for (Map.Entry<ResourcePatternFilter, Set<AccessControlEntry>> entry : filterToAcl.entrySet()) {
                ResourcePatternFilter filter = entry.getKey();
                Set<AccessControlEntry> acls = entry.getValue();
                if (acls.isEmpty()) {
                    if (confirmAction(opts, "Are you sure you want to delete all ACLs for resource filter `" + filter + "`? (y/n)")) {
                        removeAcls(admin, acls, filter);
                    }
                } else {
                    String msg = "Are you sure you want to remove ACLs: " + NL +
                            " " + acls.stream().map(a -> "\t" + a).collect(Collectors.joining(NL)) + NL +
                            " from resource filter `" + filter + "`? (y/n)";
                    if (confirmAction(opts, msg)) {
                        removeAcls(admin, acls, filter);
                    }
                }
            }
        }

        private void listAcls(Admin admin) throws ExecutionException, InterruptedException {
            Set<ResourcePatternFilter> filters = getResourceFilter(opts, false);
            Set<KafkaPrincipal> listPrincipals = getPrincipals(opts, opts.listPrincipalsOpt);
            Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls = getAcls(admin, filters);

            if (listPrincipals.isEmpty()) {
                printResourceAcls(resourceToAcls);
            } else {
                listPrincipals.forEach(principal -> {
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
                });
            }
        }

        private static void printResourceAcls(Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls) {
            resourceToAcls.forEach((resource, acls) ->
                System.out.println("Current ACLs for resource `" + resource + "`:" + NL +
                        acls.stream().map(acl -> "\t" + acl).collect(Collectors.joining(NL)) + NL)
            );
        }

        private static void removeAcls(Admin adminClient, Set<AccessControlEntry> acls, ResourcePatternFilter filter) throws ExecutionException, InterruptedException {
            if (acls.isEmpty()) {
                adminClient.deleteAcls(Collections.singletonList(new AclBindingFilter(filter, AccessControlEntryFilter.ANY))).all().get();
            } else {
                List<AclBindingFilter> aclBindingFilters = acls.stream().map(acl -> new AclBindingFilter(filter, acl.toFilter())).toList();
                adminClient.deleteAcls(aclBindingFilters).all().get();
            }
        }

        private Map<ResourcePattern, Set<AccessControlEntry>> getAcls(Admin adminClient, Set<ResourcePatternFilter> filters) throws ExecutionException, InterruptedException {
            Collection<AclBinding> aclBindings;
            if (filters.isEmpty()) {
                aclBindings = adminClient.describeAcls(AclBindingFilter.ANY).values().get();
            } else {
                aclBindings = new ArrayList<>();
                for (ResourcePatternFilter filter : filters) {
                    aclBindings.addAll(adminClient.describeAcls(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)).values().get());
                }
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
    }

    private static Map<ResourcePattern, Set<AccessControlEntry>> getResourceToAcls(AclCommandOptions opts) {
        PatternType patternType = opts.options.valueOf(opts.resourcePatternType);
        if (!patternType.isSpecific()) {
            CommandLineUtils.printUsageAndExit(opts.parser, "A '--resource-pattern-type' value of '" + patternType + "' is not valid when adding acls.");
        }
        Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcl = getResourceFilterToAcls(opts).entrySet().stream()
                .collect(Collectors.toMap(entry -> new ResourcePattern(entry.getKey().resourceType(), entry.getKey().name(), entry.getKey().patternType()),
                                          Map.Entry::getValue));

        if (resourceToAcl.values().stream().anyMatch(Set::isEmpty)) {
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

    private static Map<ResourcePatternFilter, Set<AccessControlEntry>> getProducerResourceFilterToAcls(AclCommandOptions opts) {
        Set<ResourcePatternFilter> filters = getResourceFilter(opts, true);

        Set<ResourcePatternFilter> topics = filters.stream().filter(f -> f.resourceType() == ResourceType.TOPIC).collect(Collectors.toSet());
        Set<ResourcePatternFilter> transactionalIds = filters.stream().filter(f -> f.resourceType() == ResourceType.TRANSACTIONAL_ID).collect(Collectors.toSet());
        boolean enableIdempotence = opts.options.has(opts.idempotentOpt);

        Set<AccessControlEntry> topicAcls = getAcl(opts, new HashSet<>(Arrays.asList(WRITE, DESCRIBE, CREATE)));
        Set<AccessControlEntry> transactionalIdAcls = getAcl(opts, new HashSet<>(Arrays.asList(WRITE, DESCRIBE)));

        //Write, Describe, Create permission on topics, Write, Describe on transactionalIds
        Map<ResourcePatternFilter, Set<AccessControlEntry>> result = new HashMap<>();
        for (ResourcePatternFilter topic : topics) {
            result.put(topic, topicAcls);
        }
        for (ResourcePatternFilter transactionalId : transactionalIds) {
            result.put(transactionalId, transactionalIdAcls);
        }
        if (enableIdempotence) {
            result.put(CLUSTER_RESOURCE_FILTER, getAcl(opts, Collections.singleton(IDEMPOTENT_WRITE)));
        }
        return result;
    }

    private static Map<ResourcePatternFilter, Set<AccessControlEntry>> getConsumerResourceFilterToAcls(AclCommandOptions opts) {
        Set<ResourcePatternFilter> filters = getResourceFilter(opts, true);
        Set<ResourcePatternFilter> topics = filters.stream().filter(f -> f.resourceType() == ResourceType.TOPIC).collect(Collectors.toSet());
        Set<ResourcePatternFilter> groups = filters.stream().filter(f -> f.resourceType() == ResourceType.GROUP).collect(Collectors.toSet());

        //Read, Describe on topic, Read on consumerGroup
        Set<AccessControlEntry> topicAcls = getAcl(opts, new HashSet<>(Arrays.asList(READ, DESCRIBE)));
        Set<AccessControlEntry> groupAcls = getAcl(opts, Collections.singleton(READ));

        Map<ResourcePatternFilter, Set<AccessControlEntry>> result = new HashMap<>();
        for (ResourcePatternFilter topic : topics) {
            result.put(topic, topicAcls);
        }
        for (ResourcePatternFilter group : groups) {
            result.put(group, groupAcls);
        }
        return result;
    }

    private static Map<ResourcePatternFilter, Set<AccessControlEntry>> getCliResourceFilterToAcls(AclCommandOptions opts) {
        Set<AccessControlEntry> acls = getAcl(opts);
        Set<ResourcePatternFilter> filters = getResourceFilter(opts, true);
        return filters.stream().collect(Collectors.toMap(filter -> filter, filter -> acls));
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

    private static Set<AccessControlEntry> getAcl(AclCommandOptions opts) {
        Set<AclOperation> operations = opts.options.valuesOf(opts.operationsOpt)
                .stream().map(operation -> SecurityUtils.operation(operation.trim()))
                .collect(Collectors.toSet());
        return getAcl(opts, operations);
    }

    static Set<AccessControlEntry> getAcls(Set<KafkaPrincipal> principals,
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

    private static Set<String> getHosts(AclCommandOptions opts, OptionSpec<String> hostOptionSpec, OptionSpec<String> principalOptionSpec) {
        if (opts.options.has(hostOptionSpec)) {
            return opts.options.valuesOf(hostOptionSpec).stream().map(String::trim).collect(Collectors.toSet());
        } else if (opts.options.has(principalOptionSpec)) {
            return Collections.singleton(AclEntry.WILDCARD_HOST);
        } else {
            return Collections.emptySet();
        }
    }

    private static Set<KafkaPrincipal> getPrincipals(AclCommandOptions opts, OptionSpec<String> principalOptionSpec) {
        if (opts.options.has(principalOptionSpec)) {
            return opts.options.valuesOf(principalOptionSpec).stream()
                    .map(s -> SecurityUtils.parseKafkaPrincipal(s.trim()))
                    .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    private static Set<ResourcePatternFilter> getResourceFilter(AclCommandOptions opts, boolean dieIfNoResourceFound) {
        PatternType patternType = opts.options.valueOf(opts.resourcePatternType);
        Set<ResourcePatternFilter> resourceFilters = new HashSet<>();
        if (opts.options.has(opts.topicOpt)) {
            opts.options.valuesOf(opts.topicOpt).forEach(topic -> resourceFilters.add(new ResourcePatternFilter(ResourceType.TOPIC, topic.trim(), patternType)));
        }
        if (patternType == PatternType.LITERAL && (opts.options.has(opts.clusterOpt) || opts.options.has(opts.idempotentOpt))) {
            resourceFilters.add(CLUSTER_RESOURCE_FILTER);
        }
        if (opts.options.has(opts.groupOpt)) {
            opts.options.valuesOf(opts.groupOpt).forEach(group -> resourceFilters.add(new ResourcePatternFilter(ResourceType.GROUP, group.trim(), patternType)));
        }
        if (opts.options.has(opts.transactionalIdOpt)) {
            opts.options.valuesOf(opts.transactionalIdOpt).forEach(transactionalId ->
                    resourceFilters.add(new ResourcePatternFilter(ResourceType.TRANSACTIONAL_ID, transactionalId, patternType)));
        }
        if (opts.options.has(opts.delegationTokenOpt)) {
            opts.options.valuesOf(opts.delegationTokenOpt).forEach(token -> resourceFilters.add(new ResourcePatternFilter(ResourceType.DELEGATION_TOKEN, token.trim(), patternType)));
        }
        if (opts.options.has(opts.userPrincipalOpt)) {
            opts.options.valuesOf(opts.userPrincipalOpt).forEach(user -> resourceFilters.add(new ResourcePatternFilter(ResourceType.USER, user.trim(), patternType)));
        }
        if (resourceFilters.isEmpty() && dieIfNoResourceFound) {
            CommandLineUtils.printUsageAndExit(opts.parser, "You must provide at least one resource: --topic <topic> or --cluster or --group <group> or --delegation-token <Delegation Token ID>");
        }
        return resourceFilters;
    }

    private static boolean confirmAction(AclCommandOptions opts, String msg) {
        if (opts.options.has(opts.forceOpt)) {
            return true;
        }
        System.out.println(msg);
        return System.console().readLine().equalsIgnoreCase("y");
    }

    private static void validateOperation(AclCommandOptions opts, Map<ResourcePatternFilter, Set<AccessControlEntry>> resourceToAcls) {
        for (Map.Entry<ResourcePatternFilter, Set<AccessControlEntry>> entry : resourceToAcls.entrySet()) {
            ResourcePatternFilter resource = entry.getKey();
            Set<AccessControlEntry> acls = entry.getValue();
            Collection<AclOperation> validOps = new HashSet<>(AclEntry.supportedOperations(resource.resourceType()));
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

    public static class AclCommandOptions extends CommandDefaultOptions {

        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> bootstrapControllerOpt;
        private final OptionSpec<String> commandConfigOpt;
        private final OptionSpec<String> topicOpt;
        private final OptionSpecBuilder clusterOpt;
        private final OptionSpec<String> groupOpt;
        private final OptionSpec<String> transactionalIdOpt;
        private final OptionSpecBuilder idempotentOpt;
        private final OptionSpec<String> delegationTokenOpt;
        private final OptionSpec<PatternType> resourcePatternType;
        private final OptionSpecBuilder addOpt;
        private final OptionSpecBuilder removeOpt;
        private final OptionSpecBuilder listOpt;
        private final OptionSpec<String> operationsOpt;
        private final OptionSpec<String> allowPrincipalsOpt;
        private final OptionSpec<String> denyPrincipalsOpt;
        private final OptionSpec<String> listPrincipalsOpt;
        private final OptionSpec<String> allowHostsOpt;
        private final OptionSpec<String> denyHostsOpt;
        private final OptionSpecBuilder producerOpt;
        private final OptionSpecBuilder consumerOpt;
        private final OptionSpecBuilder forceOpt;
        private final OptionSpec<String> userPrincipalOpt;

        @SuppressWarnings("this-escape")
        public AclCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "A list of host/port pairs to use for establishing the connection to the Kafka cluster." +
                            " This list should be in the form host1:port1,host2:port2,... This config is required for acl management using admin client API.")
                    .withRequiredArg()
                    .describedAs("server to connect to")
                    .ofType(String.class);
            bootstrapControllerOpt = parser.accepts("bootstrap-controller", "A list of host/port pairs to use for establishing the connection to the Kafka cluster." +
                            " This list should be in the form host1:port1,host2:port2,... This config is required for acl management using admin client API.")
                    .withRequiredArg()
                    .describedAs("controller to connect to")
                    .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "A property file containing configs to be passed to Admin Client.")
                    .withOptionalArg()
                    .describedAs("command-config")
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
                    .withValuesConvertedBy(new PatternTypeConverter())
                    .defaultsTo(PatternType.LITERAL);
            addOpt = parser.accepts("add", "Indicates you are trying to add ACLs.");
            removeOpt = parser.accepts("remove", "Indicates you are trying to remove ACLs.");
            listOpt = parser.accepts("list", "List ACLs for the specified resource, use --topic <topic> or --group <group> or --cluster to specify a resource.");
            operationsOpt = parser.accepts("operation", "Operation that is being allowed or denied. Valid operation names are: " + NL +
                            AclEntry.ACL_OPERATIONS.stream().map(o -> "\t" + SecurityUtils.operationName(o)).collect(Collectors.joining(NL)) + NL)
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
            userPrincipalOpt = parser.accepts("user-principal", "Specifies a user principal as a resource in relation with the operation. For instance " +
                            "one could grant CreateTokens or DescribeTokens permission on a given user principal.")
                    .withRequiredArg()
                    .describedAs("user-principal")
                    .ofType(String.class);

            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }
            checkArgs();
        }

        void checkArgs() {
            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to manage acls on kafka.");

            if (options.has(bootstrapServerOpt) && options.has(bootstrapControllerOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "Only one of --bootstrap-server or --bootstrap-controller must be specified");
            }
            if (!options.has(bootstrapServerOpt) && !options.has(bootstrapControllerOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "One of --bootstrap-server or --bootstrap-controller must be specified");
            }
            List<AbstractOptionSpec<?>> mutuallyExclusiveOptions = Arrays.asList(addOpt, removeOpt, listOpt);
            long mutuallyExclusiveOptionsCount = mutuallyExclusiveOptions.stream()
                    .filter(abstractOptionSpec -> options.has(abstractOptionSpec))
                    .count();
            if (mutuallyExclusiveOptionsCount != 1) {
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --list, --add, --remove. ");
            }
            CommandLineUtils.checkInvalidArgs(parser, options, listOpt, producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostsOpt, denyPrincipalsOpt);

            //when --producer or --consumer is specified , user should not specify operations as they are inferred and we also disallow --deny-principals and --deny-hosts.
            CommandLineUtils.checkInvalidArgs(parser, options, producerOpt, operationsOpt, denyPrincipalsOpt, denyHostsOpt);
            CommandLineUtils.checkInvalidArgs(parser, options, consumerOpt, operationsOpt, denyPrincipalsOpt, denyHostsOpt);

            if (options.has(listPrincipalsOpt) && !options.has(listOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "The --principal option is only available if --list is set");
            }
            if (options.has(producerOpt) && !options.has(topicOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "With --producer you must specify a --topic");
            }
            if (options.has(idempotentOpt) && !options.has(producerOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "The --idempotent option is only available if --producer is set");
            }
            if (options.has(consumerOpt) && (!options.has(topicOpt) || !options.has(groupOpt) || (!options.has(producerOpt) && (options.has(clusterOpt) || options.has(transactionalIdOpt))))) {
                CommandLineUtils.printUsageAndExit(parser, "With --consumer you must specify a --topic and a --group and no --cluster or --transactional-id option should be specified.");
            }
        }
    }

    static class PatternTypeConverter extends EnumConverter<PatternType> {

        PatternTypeConverter() {
            super(PatternType.class);
        }

        @Override
        public PatternType convert(String value) {
            PatternType patternType = super.convert(value);
            if (patternType.isUnknown())
                throw new ValueConversionException("Unknown resource-pattern-type: " + value);

            return patternType;
        }

        @Override
        public String valuePattern() {
            List<PatternType> values = Arrays.asList(PatternType.values());
            List<PatternType> filteredValues = values.stream()
                    .filter(type -> type != PatternType.UNKNOWN)
                    .toList();
            return filteredValues.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining("|"));
        }
    }
}
