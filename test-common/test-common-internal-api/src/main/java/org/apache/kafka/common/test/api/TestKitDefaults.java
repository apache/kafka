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

import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * Constants used by TestKitNodes and ClusterTest annotation defaults
 */
public class TestKitDefaults {
    public static final int CONTROLLER_ID_OFFSET = 3000;
    public static final int BROKER_ID_OFFSET = 0;
    public static final SecurityProtocol DEFAULT_BROKER_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;
    public static final String DEFAULT_BROKER_LISTENER_NAME = "EXTERNAL";
    public static final SecurityProtocol DEFAULT_CONTROLLER_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;
    public static final String DEFAULT_CONTROLLER_LISTENER_NAME = "CONTROLLER";

    private TestKitDefaults() {

    }
}
