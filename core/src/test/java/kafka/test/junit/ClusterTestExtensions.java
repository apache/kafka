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

package kafka.test.junit;

import kafka.test.ClusterConfig;
import kafka.test.ClusterGenerator;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.ClusterTests;
import kafka.test.annotation.ClusterTemplate;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.Type;
import kafka.test.annotation.AutoStart;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.function.Consumer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a custom JUnit extension that will generate some number of test invocations depending on the processing
 * of a few custom annotations. These annotations are placed on so-called test template methods. Template methods look
 * like normal JUnit test methods, but instead of being invoked directly, they are used as templates for generating
 * multiple test invocations.
 *
 * Test class that use this extension should use one of the following annotations on each template method:
 *
 * <ul>
 *     <li>{@link ClusterTest}, define a single cluster configuration</li>
 *     <li>{@link ClusterTests}, provide multiple instances of @ClusterTest</li>
 *     <li>{@link ClusterTemplate}, define a static method that generates cluster configurations</li>
 * </ul>
 *
 * Any combination of these annotations may be used on a given test template method. If no test invocations are
 * generated after processing the annotations, an error is thrown.
 *
 * Depending on which annotations are used, and what values are given, different {@link ClusterConfig} will be
 * generated. Each ClusterConfig is used to create an underlying Kafka cluster that is used for the actual test
 * invocation.
 *
 * For example:
 *
 * <pre>
 * &#64;ExtendWith(value = Array(classOf[ClusterTestExtensions]))
 * class SomeIntegrationTest {
 *   &#64;ClusterTest(brokers = 1, controllers = 1, clusterType = ClusterType.Both)
 *   def someTest(): Unit = {
 *     assertTrue(condition)
 *   }
 * }
 * </pre>
 *
 * will generate two invocations of "someTest" (since ClusterType.Both was given). For each invocation, the test class
 * SomeIntegrationTest will be instantiated, lifecycle methods (before/after) will be run, and "someTest" will be invoked.
 *
 */
public class ClusterTestExtensions implements TestTemplateInvocationContextProvider {
    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        ClusterTestDefaults defaults = getClusterTestDefaults(context.getRequiredTestClass());
        List<TestTemplateInvocationContext> generatedContexts = new ArrayList<>();

        // Process the @ClusterTemplate annotation
        ClusterTemplate clusterTemplateAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTemplate.class);
        if (clusterTemplateAnnot != null) {
            processClusterTemplate(context, clusterTemplateAnnot, generatedContexts::add);
            if (generatedContexts.isEmpty()) {
                throw new IllegalStateException("ClusterConfig generator method should provide at least one config");
            }
        }

        // Process single @ClusterTest annotation
        ClusterTest clusterTestAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTest.class);
        if (clusterTestAnnot != null) {
            processClusterTest(context, clusterTestAnnot, defaults, generatedContexts::add);
        }

        // Process multiple @ClusterTest annotation within @ClusterTests
        ClusterTests clusterTestsAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTests.class);
        if (clusterTestsAnnot != null) {
            for (ClusterTest annot : clusterTestsAnnot.value()) {
                processClusterTest(context, annot, defaults, generatedContexts::add);
            }
        }

        if (generatedContexts.isEmpty()) {
            throw new IllegalStateException("Please annotate test methods with @ClusterTemplate, @ClusterTest, or " +
                    "@ClusterTests when using the ClusterTestExtensions provider");
        }

        return generatedContexts.stream();
    }

    void processClusterTemplate(ExtensionContext context, ClusterTemplate annot,
                                        Consumer<TestTemplateInvocationContext> testInvocations) {
        // If specified, call cluster config generated method (must be static)
        List<ClusterConfig> generatedClusterConfigs = new ArrayList<>();
        if (annot.value().trim().isEmpty()) {
            throw new IllegalStateException("ClusterTemplate value can't be empty string.");
        }
        generateClusterConfigurations(context, annot.value(), generatedClusterConfigs::add);

        String baseDisplayName = context.getRequiredTestMethod().getName();
        generatedClusterConfigs.forEach(config -> {
            for (Type type: config.clusterTypes()) {
                type.invocationContexts(baseDisplayName, config, testInvocations);
            }
        });
    }

    private void generateClusterConfigurations(ExtensionContext context, String generateClustersMethods, ClusterGenerator generator) {
        Object testInstance = context.getTestInstance().orElse(null);
        Method method = ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), generateClustersMethods, ClusterGenerator.class);
        ReflectionUtils.invokeMethod(method, testInstance, generator);
    }

    private void processClusterTest(ExtensionContext context, ClusterTest annot, ClusterTestDefaults defaults,
                                    Consumer<TestTemplateInvocationContext> testInvocations) {
        Type[] types = annot.types().length == 0 ? defaults.types() : annot.types();
        Map<String, String> serverProperties = Stream.concat(Arrays.stream(defaults.serverProperties()), Arrays.stream(annot.serverProperties()))
                .filter(e -> e.id() == -1)
                .collect(Collectors.toMap(ClusterConfigProperty::key, ClusterConfigProperty::value, (a, b) -> b));

        Map<Integer, Map<String, String>> perServerProperties = Stream.concat(Arrays.stream(defaults.serverProperties()), Arrays.stream(annot.serverProperties()))
                .filter(e -> e.id() != -1)
                .collect(Collectors.groupingBy(ClusterConfigProperty::id, Collectors.mapping(Function.identity(),
                        Collectors.toMap(ClusterConfigProperty::key, ClusterConfigProperty::value, (a, b) -> b))));
        ClusterConfig config = ClusterConfig.builder()
                .setTypes(new HashSet<>(Arrays.asList(types)))
                .setBrokers(annot.brokers() == 0 ? defaults.brokers() : annot.brokers())
                .setControllers(annot.controllers() == 0 ? defaults.controllers() : annot.controllers())
                .setDisksPerBroker(annot.disksPerBroker() == 0 ? defaults.disksPerBroker() : annot.disksPerBroker())
                .setAutoStart(annot.autoStart() == AutoStart.DEFAULT ? defaults.autoStart() : annot.autoStart() == AutoStart.YES)
                .setListenerName(annot.listener().trim().isEmpty() ? null : annot.listener())
                .setServerProperties(serverProperties)
                .setPerServerProperties(perServerProperties)
                .setSecurityProtocol(annot.securityProtocol())
                .setMetadataVersion(annot.metadataVersion())
                .setTags(Arrays.asList(annot.tags()))
                .build();
        for (Type type : types) {
            type.invocationContexts(context.getRequiredTestMethod().getName(), config, testInvocations);
        }
    }

    private ClusterTestDefaults getClusterTestDefaults(Class<?> testClass) {
        return Optional.ofNullable(testClass.getDeclaredAnnotation(ClusterTestDefaults.class))
            .orElseGet(() -> EmptyClass.class.getDeclaredAnnotation(ClusterTestDefaults.class));
    }

    @ClusterTestDefaults
    private final static class EmptyClass {
        // Just used as a convenience to get default values from the annotation
    }
}
