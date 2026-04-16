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
package org.apache.kafka.jmh.metadata;

import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.image.ClientQuotaImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.server.config.QuotaConfig;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ClientQuotasImageDescribeBenchmark {

    @Param({"10", "100", "1000"})
    private int eachEntityCount;

    private ClientQuotasImage clientQuotasImage;

    @Setup(Level.Trial)
    public void setup() {
        clientQuotasImage = createClientQuotasImage(eachEntityCount);
    }

    static ClientQuotasImage createClientQuotasImage(int eachEntityCount) {
        Map<ClientQuotaEntity, ClientQuotaImage> entities = new HashMap<>();
        ClientQuotaImage defaultImage = new ClientQuotaImage(Map.of(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 1.0));
        for (int i = 0; i < eachEntityCount; i++) {
            entities.put(new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user-" + i)), defaultImage);
            entities.put(new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "client-id-" + i)), defaultImage);
            entities.put(new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, "ip-" + i)), defaultImage);
        }
        return new ClientQuotasImage(entities);
    }

    @Benchmark
    public DescribeClientQuotasResponseData describeSpecified() {
        return clientQuotasImage.describe(new DescribeClientQuotasRequestData()
            .setComponents(List.of(new DescribeClientQuotasRequestData.ComponentData()
                .setEntityType(ClientQuotaEntity.USER)
                .setMatchType(DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED)
                .setMatch(null))));
    }

    @Benchmark
    public DescribeClientQuotasResponseData describeDefault() {
        return clientQuotasImage.describe(new DescribeClientQuotasRequestData()
            .setComponents(List.of(new DescribeClientQuotasRequestData.ComponentData()
                .setEntityType(ClientQuotaEntity.USER)
                .setMatchType(DescribeClientQuotasRequest.MATCH_TYPE_DEFAULT)
                .setMatch(null))));
    }

    @Benchmark
    public DescribeClientQuotasResponseData describeExact() {
        return clientQuotasImage.describe(new DescribeClientQuotasRequestData()
            .setComponents(List.of(new DescribeClientQuotasRequestData.ComponentData()
                .setEntityType(ClientQuotaEntity.USER)
                .setMatchType(DescribeClientQuotasRequest.MATCH_TYPE_EXACT)
                .setMatch("user-0"))));
    }
}
