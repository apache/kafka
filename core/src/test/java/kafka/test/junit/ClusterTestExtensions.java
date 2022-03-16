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
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTemplate;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTests;
import kafka.test.annotation.Type;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
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
 **/
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
            if (generatedContexts.size() == 0) {
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

        if (generatedContexts.size() == 0) {
            throw new IllegalStateException("Please annotate test methods with @ClusterTemplate, @ClusterTest, or " +
                    "@ClusterTests when using the ClusterTestExtensions provider");
        }

        return generatedContexts.stream();
    }

    private void processClusterTemplate(ExtensionContext context, ClusterTemplate annot,
                                        Consumer<TestTemplateInvocationContext> testInvocations) {
        // If specified, call cluster config generated method (must be static)
        List<ClusterConfig> generatedClusterConfigs = new ArrayList<>();
        if (!annot.value().isEmpty()) {
            generateClusterConfigurations(context, annot.value(), generatedClusterConfigs::add);
        } else {
            // Ensure we have at least one cluster config
            generatedClusterConfigs.add(ClusterConfig.defaultClusterBuilder().build());
        }

        generatedClusterConfigs.forEach(config -> config.clusterType().invocationContexts(config, testInvocations));
    }

    private void generateClusterConfigurations(ExtensionContext context, String generateClustersMethods, ClusterGenerator generator) {
        Object testInstance = context.getTestInstance().orElse(null);
        Method method = ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), generateClustersMethods, ClusterGenerator.class);
        ReflectionUtils.invokeMethod(method, testInstance, generator);
    }

    private void processClusterTest(ExtensionContext context, ClusterTest annot, ClusterTestDefaults defaults,
                                    Consumer<TestTemplateInvocationContext> testInvocations) {
        final Type type;
        if (annot.clusterType() == Type.DEFAULT) {
            type = defaults.clusterType();
        } else {
            type = annot.clusterType();
        }

        final int brokers;
        if (annot.brokers() == 0) {
            brokers = defaults.brokers();
        } else {
            brokers = annot.brokers();
        }

        final int controllers;
        if (annot.controllers() == 0) {
            controllers = defaults.controllers();
        } else {
            controllers = annot.controllers();
        }

        if (brokers <= 0 || controllers <= 0) {
            throw new IllegalArgumentException("Number of brokers/controllers must be greater than zero.");
        }

        final boolean autoStart;
        switch (annot.autoStart()) {
            case YES:
                autoStart = true;
                break;
            case NO:
                autoStart = false;
                break;
            case DEFAULT:
                autoStart = defaults.autoStart();
                break;
            default:
                throw new IllegalStateException();
        }

        ClusterConfig.Builder builder = ClusterConfig.clusterBuilder(type, brokers, controllers, autoStart, annot.securityProtocol());
        if (!annot.name().isEmpty()) {
            builder.name(annot.name());
        } else {
            builder.name(context.getRequiredTestMethod().getName());
        }
        if (!annot.listener().isEmpty()) {
            builder.listenerName(annot.listener());
        }

        Properties properties = new Properties();
        for (ClusterConfigProperty property : annot.serverProperties()) {
            properties.put(property.key(), property.value());
        }

        if (!annot.ibp().isEmpty()) {
            builder.ibp(annot.ibp());
        }

        ClusterConfig config = builder.build();
        config.serverProperties().putAll(properties);
        type.invocationContexts(config, testInvocations);
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
