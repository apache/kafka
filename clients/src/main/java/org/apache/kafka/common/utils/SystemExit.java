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
package org.apache.kafka.common.utils;

final class SystemExit extends Exit {

    private static final Exit SYSTEM = new SystemExit();

    private SystemExit() {
    }

    public static Exit instance() {
        return SYSTEM;
    }

    @Override
    public void exitOrThrow(int statusCode, String message) {
        System.exit(statusCode);
    }

    @Override
    public void haltOrThrow(int statusCode, String message) {
        Runtime.getRuntime().halt(statusCode);
    }

    @Override
    public void addShutdownRunnable(String name, Runnable runnable) {
        if (name != null)
            Runtime.getRuntime().addShutdownHook(KafkaThread.nonDaemon(name, runnable));
        else
            Runtime.getRuntime().addShutdownHook(new Thread(runnable));
    }
}
