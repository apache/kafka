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

package org.apache.kafka.common.security.ssl;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.Configurable;

/**
 * A SSLContext provider interface to create SSLContext based on configs
 */
public interface SslContextProvider extends Configurable {

    /**
    * Create a SSLContext based on protocol and provider config.
    * @return SSLContext
    * @throws NoSuchAlgorithmException if protocol config is invalid.
    * @throws NoSuchProviderException if provider config is invalid.
    */
    SSLContext getSSLContext() throws NoSuchAlgorithmException, NoSuchProviderException;
}
