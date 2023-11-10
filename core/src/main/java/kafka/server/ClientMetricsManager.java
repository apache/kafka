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
package kafka.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

/**
 * Handles client telemetry metrics requests/responses, subscriptions and instance information.
 */
public class ClientMetricsManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClientMetricsManager.class);
    private static final ClientMetricsManager INSTANCE = new ClientMetricsManager();

    public static ClientMetricsManager instance() {
        return INSTANCE;
    }

    public void updateSubscription(String subscriptionName, Properties properties) {
        // TODO: Implement the update logic to manage subscriptions.
    }

    @Override
    public void close() throws IOException {
        // TODO: Implement the close logic to close the client metrics manager.
    }
}
