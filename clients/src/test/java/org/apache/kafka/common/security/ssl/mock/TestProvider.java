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
package org.apache.kafka.common.security.ssl.mock;

import java.security.Provider;

public final class TestProvider extends Provider {

    private static final String KEY_MANAGER_FACTORY = String.format("KeyManagerFactory.%s", TestKeyManagerFactory.ALGORITHM);
    private static final String TRUST_MANAGER_FACTORY = String.format("TrustManagerFactory.%s", TestTrustManagerFactory.ALGORITHM);

    public TestProvider() {
        this("TestProvider", "0.1", "provider for test cases");
    }

    private TestProvider(String name, String version, String info) {
        super(name, version, info);
        super.put(KEY_MANAGER_FACTORY, TestKeyManagerFactory.class.getName());
        super.put(TRUST_MANAGER_FACTORY, TestTrustManagerFactory.class.getName());
    }

}
