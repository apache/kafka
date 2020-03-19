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

package org.apache.kafka.clients.admin;

import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Options for {@link Admin#createDelegationToken(CreateDelegationTokenOptions)}.
 */
public class CreateDelegationTokenOptions extends AbstractOptions<CreateDelegationTokenOptions> {
    private long maxLifeTimeMs = -1;
    private List<KafkaPrincipal> renewers =  new LinkedList<>();

    public CreateDelegationTokenOptions renewers(List<KafkaPrincipal> renewers) {
        this.renewers = renewers;
        return this;
    }

    public List<KafkaPrincipal> renewers() {
        return renewers;
    }

    public CreateDelegationTokenOptions maxlifeTimeMs(long maxLifeTimeMs) {
        this.maxLifeTimeMs = maxLifeTimeMs;
        return this;
    }

    public long maxlifeTimeMs() {
        return maxLifeTimeMs;
    }
}
