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
package org.apache.kafka.common.test.api;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.util.timer.SystemTimer;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * A special system property "kafka.cluster.test.repeat" can be used to cause repeated invocation of the tests.
 *
 * For example:
 *
 * <pre>
 * ./gradlew -Pkafka.cluster.test.repeat=3 :core:test
 * </pre>
 *
 * will cause all ClusterTest-s in the :core module to be invoked three times.
 */
public class ClusterTestExtensions implements TestTemplateInvocationContextProvider, BeforeEachCallback, AfterEachCallback {
    public static final String CLUSTER_TEST_REPEAT_SYSTEM_PROP = "kafka.cluster.test.repeat";

    private static final String METRICS_METER_TICK_THREAD_PREFIX = "metrics-meter-tick-thread";
    private static final String SCALA_THREAD_PREFIX = "scala-";
    private static final String FORK_JOIN_POOL_THREAD_PREFIX = "ForkJoinPool";
    private static final String JUNIT_THREAD_PREFIX = "junit-";
    private static final String ATTACH_LISTENER_THREAD_PREFIX = "Attach Listener";
    private static final String PROCESS_REAPER_THREAD_PREFIX = "process reaper";
    private static final String RMI_THREAD_PREFIX = "RMI";
    private static final String DETECT_THREAD_LEAK_KEY = "detectThreadLeak";
    private static final Set<String> SKIPPED_THREAD_PREFIX = Collections.unmodifiableSet(Stream.of(
            METRICS_METER_TICK_THREAD_PREFIX, SCALA_THREAD_PREFIX, FORK_JOIN_POOL_THREAD_PREFIX, JUNIT_THREAD_PREFIX,
            ATTACH_LISTENER_THREAD_PREFIX, PROCESS_REAPER_THREAD_PREFIX, RMI_THREAD_PREFIX, SystemTimer.SYSTEM_TIMER_THREAD_PREFIX)
            .collect(Collectors.toSet()));

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
            generatedContexts.addAll(processClusterTemplate(context, clusterTemplateAnnot));
        }

        // Process single @ClusterTest annotation
        ClusterTest clusterTestAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTest.class);
        if (clusterTestAnnot != null) {
            generatedContexts.addAll(processClusterTests(context, new ClusterTest[]{clusterTestAnnot}, defaults));
        }

        // Process multiple @ClusterTest annotation within @ClusterTests
        ClusterTests clusterTestsAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTests.class);
        if (clusterTestsAnnot != null) {
            generatedContexts.addAll(processClusterTests(context, clusterTestsAnnot.value(), defaults));
        }

        if (generatedContexts.isEmpty()) {
            throw new IllegalStateException("Please annotate test methods with @ClusterTemplate, @ClusterTest, or " +
                    "@ClusterTests when using the ClusterTestExtensions provider");
        }

        return generatedContexts.stream();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        DetectThreadLeak detectThreadLeak = DetectThreadLeak.of(thread ->
                SKIPPED_THREAD_PREFIX.stream().noneMatch(prefix -> thread.getName().startsWith(prefix)));
        getStore(context).put(DETECT_THREAD_LEAK_KEY, detectThreadLeak);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        DetectThreadLeak detectThreadLeak = getStore(context).remove(DETECT_THREAD_LEAK_KEY, DetectThreadLeak.class);
        if (detectThreadLeak == null) {
            return;
        }
        List<Thread> threads = detectThreadLeak.newThreads();
        assertTrue(threads.isEmpty(), "Thread leak detected: " +
                threads.stream().map(Thread::getName).collect(Collectors.joining(", ")));
    }

    private Store getStore(ExtensionContext context) {
        return context.getStore(Namespace.create(context.getUniqueId()));
    }

    private int getTestRepeatCount() {
        int count;
        try {
            String repeatCount = System.getProperty(CLUSTER_TEST_REPEAT_SYSTEM_PROP, "1");
            count = Integer.parseInt(repeatCount);
        } catch (NumberFormatException e) {
            count = 1;
        }
        return count;
    }

    List<TestTemplateInvocationContext> processClusterTemplate(ExtensionContext context, ClusterTemplate annot) {
        if (annot.value().trim().isEmpty()) {
            throw new IllegalStateException("ClusterTemplate value can't be empty string.");
        }

        String baseDisplayName = context.getRequiredTestMethod().getName();
        int repeatCount = getTestRepeatCount();
        List<TestTemplateInvocationContext> contexts = IntStream.range(0, repeatCount)
            .mapToObj(__ -> generateClusterConfigurations(context, annot.value()).stream())
            .flatMap(Function.identity())
            .flatMap(config -> config.clusterTypes().stream().map(type -> type.invocationContexts(baseDisplayName, config)))
            .collect(Collectors.toList());

        if (contexts.isEmpty()) {
            throw new IllegalStateException("ClusterConfig generator method should provide at least one config");
        }

        return contexts;
    }

    @SuppressWarnings("unchecked")
    private List<ClusterConfig> generateClusterConfigurations(
        ExtensionContext context,
        String generateClustersMethods
    ) {
        Object testInstance = context.getTestInstance().orElse(null);
        Method method = ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), generateClustersMethods);
        return (List<ClusterConfig>) ReflectionUtils.invokeMethod(method, testInstance);
    }

    private List<TestTemplateInvocationContext> processClusterTests(
        ExtensionContext context,
        ClusterTest[] clusterTests,
        ClusterTestDefaults defaults
    ) {
        int repeatCount = getTestRepeatCount();
        List<TestTemplateInvocationContext> ret = IntStream.range(0, repeatCount)
            .mapToObj(__ -> Arrays.stream(clusterTests))
            .flatMap(Function.identity())
            .flatMap(clusterTest -> processClusterTestInternal(context, clusterTest, defaults).stream())
            .collect(Collectors.toList());

        if (ret.isEmpty()) {
            throw new IllegalStateException("processClusterTests method should provide at least one config");
        }

        return ret;
    }

    private List<TestTemplateInvocationContext> processClusterTestInternal(
        ExtensionContext context,
        ClusterTest clusterTest,
        ClusterTestDefaults defaults
    ) {
        Type[] types = clusterTest.types().length == 0 ? defaults.types() : clusterTest.types();
        Map<String, String> serverProperties = Stream.concat(Arrays.stream(defaults.serverProperties()), Arrays.stream(clusterTest.serverProperties()))
            .filter(e -> e.id() == -1)
            .collect(Collectors.toMap(ClusterConfigProperty::key, ClusterConfigProperty::value, (a, b) -> b));

        Map<Integer, Map<String, String>> perServerProperties = Stream.concat(Arrays.stream(defaults.serverProperties()), Arrays.stream(clusterTest.serverProperties()))
            .filter(e -> e.id() != -1)
            .collect(Collectors.groupingBy(ClusterConfigProperty::id, Collectors.mapping(Function.identity(),
                Collectors.toMap(ClusterConfigProperty::key, ClusterConfigProperty::value, (a, b) -> b))));

        Map<Feature, Short> features = Arrays.stream(clusterTest.features())
            .collect(Collectors.toMap(ClusterFeature::feature, ClusterFeature::version));

        ClusterConfig config = ClusterConfig.builder()
            .setTypes(new HashSet<>(Arrays.asList(types)))
            .setBrokers(clusterTest.brokers() == 0 ? defaults.brokers() : clusterTest.brokers())
            .setControllers(clusterTest.controllers() == 0 ? defaults.controllers() : clusterTest.controllers())
            .setDisksPerBroker(clusterTest.disksPerBroker() == 0 ? defaults.disksPerBroker() : clusterTest.disksPerBroker())
            .setAutoStart(clusterTest.autoStart() == AutoStart.DEFAULT ? defaults.autoStart() : clusterTest.autoStart() == AutoStart.YES)
            .setBrokerListenerName(ListenerName.normalised(clusterTest.brokerListener()))
            .setBrokerSecurityProtocol(clusterTest.brokerSecurityProtocol())
            .setControllerListenerName(ListenerName.normalised(clusterTest.controllerListener()))
            .setControllerSecurityProtocol(clusterTest.controllerSecurityProtocol())
            .setServerProperties(serverProperties)
            .setPerServerProperties(perServerProperties)
            .setMetadataVersion(clusterTest.metadataVersion())
            .setTags(Arrays.asList(clusterTest.tags()))
            .setFeatures(features)
            .build();

        return Arrays.stream(types)
            .map(type -> type.invocationContexts(context.getRequiredTestMethod().getName(), config))
            .collect(Collectors.toList());
    }

    private ClusterTestDefaults getClusterTestDefaults(Class<?> testClass) {
        return Optional.ofNullable(testClass.getDeclaredAnnotation(ClusterTestDefaults.class))
            .orElseGet(() -> EmptyClass.class.getDeclaredAnnotation(ClusterTestDefaults.class));
    }

    @ClusterTestDefaults
    private static final class EmptyClass {
        // Just used as a convenience to get default values from the annotation
    }
}
