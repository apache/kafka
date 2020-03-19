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

import java.util.List;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Options for {@link Admin#describeDelegationToken(DescribeDelegationTokenOptions)}.
 */
public class DescribeDelegationTokenOptions extends AbstractOptions<DescribeDelegationTokenOptions> {
    private List<KafkaPrincipal> owners;

    /**
     * if owners is null, all the user owned tokens and tokens where user have Describe permission
     * will be returned.
     * @param owners
     * @return this instance
     */
    public DescribeDelegationTokenOptions owners(List<KafkaPrincipal> owners) {
        this.owners = owners;
        return this;
    }

    public List<KafkaPrincipal> owners() {
        return owners;
    }
}
