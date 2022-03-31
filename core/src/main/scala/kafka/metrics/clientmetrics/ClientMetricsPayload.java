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

package kafka.metrics.clientmetrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.ClientTelemetryPayload;

import java.nio.ByteBuffer;

public class ClientMetricsPayload implements ClientTelemetryPayload {
    final private Uuid clientInstanceId;
    final private boolean isClientTerminating;
    final private String metricsContentType;
    final private ByteBuffer metricsData;

    ClientMetricsPayload(Uuid clientId, boolean terminating, String contentType, ByteBuffer data) {
        this.clientInstanceId = clientId;
        this.isClientTerminating = terminating;
        this.metricsContentType = contentType;
        this.metricsData = data;
    }

    @Override
    public Uuid clientInstanceId() {
        return this.clientInstanceId;
    }

    @Override
    public boolean isTerminating() {
        return isClientTerminating;
    }

    @Override
    public String contentType() {
        return metricsContentType;
    }

    @Override
    public ByteBuffer data() {
        return metricsData;
    }
}
