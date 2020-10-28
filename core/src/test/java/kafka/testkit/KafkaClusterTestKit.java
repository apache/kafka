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

package kafka.testkit;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import kafka.server.KafkaConfig$;
import kafka.server.Kip500Controller;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

public class KafkaClusterTestKit implements AutoCloseable {
    public static class Builder {
        private int numControllers = 0;
        private Map<String, String> configProps = new HashMap<>();

        public Builder setNumControllers(int numControllers) {
            this.numControllers = numControllers;
            return this;
        }

        public Builder setConfigProp(String key, String value) {
            this.configProps.put(key, value);
            return this;
        }

        public KafkaClusterTestKit build() throws InterruptedException {
            if (numControllers == 0) {
                throw new RuntimeException("Currently, we only support clusters " +
                        "where there is at least one kip-500 controller.");
            } else {
                if (!configProps.containsKey(KafkaConfig$.MODULE$.ProcessRolesProp())) {
                    configProps.put(KafkaConfig$.MODULE$.ProcessRolesProp(), "controller");
                }
            }

            Map<Integer, Kip500Controller> controllers = new HashMap<>();
            ExecutorService executorService = null;
            try {
                executorService = Executors.newFixedThreadPool(numControllers,
                    ThreadUtils.createThreadFactory("KafkaClusterTestKit%d", false));
                Time time = Time.SYSTEM;
                for (int i = 0; i < numControllers; i++) {
                    Map<String, String> controllerProps = new HashMap<>(configProps);
                    controllerProps.put(KafkaConfig$.MODULE$.ControllerIdProp(),
                        Integer.toString(i));
                    KafkaConfig config = new KafkaConfig(controllerProps, false,
                        OptionConverters.toScala(Optional.empty()));
                    controllers.put(i, new Kip500Controller(config, time,
                        OptionConverters.toScala(Optional.of(String.format("controller%d_", i))),
                        CollectionConverters.<KafkaMetricsReporter>asScala(Collections.emptyList()).toSeq()));
                }
            } catch (Exception e) {
                if (executorService != null) {
                    executorService.shutdownNow();
                    executorService.awaitTermination(1, TimeUnit.DAYS);
                }
                for (Kip500Controller controller : controllers.values()) {
                    controller.shutdown();
                }
                throw e;
            }
            return new KafkaClusterTestKit(executorService, controllers);
        }
    }

    private final ExecutorService executorService;
    private final Map<Integer, Kip500Controller> controllers;

    private KafkaClusterTestKit(ExecutorService executorService,
                                Map<Integer, Kip500Controller> controllers) {
        this.executorService = executorService;
        this.controllers = controllers;
    }

    public void startup() throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (Kip500Controller controller : controllers.values()) {
                futures.add(executorService.submit(() -> {
                    controller.startup();
                }));
            }
            for (Future<?> future: futures) {
                future.get();
            }
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        }
    }

    @Override
    public void close() throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (Kip500Controller controller : controllers.values()) {
                futures.add(executorService.submit(() -> {
                    controller.shutdown();
                }));
            }
            for (Future<?> future: futures) {
                future.get();
            }
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
    }
}
