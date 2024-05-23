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
package org.apache.kafka.server.config;

/** ********* Controlled shutdown configuration ***********/
public class ControlledShutdownConfigs {
    public static final String CONTROLLED_SHUTDOWN_MAX_RETRIES_CONFIG = "controlled.shutdown.max.retries";
    public static final String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_CONFIG = "controlled.shutdown.retry.backoff.ms";
    public static final String CONTROLLED_SHUTDOWN_ENABLE_CONFIG = "controlled.shutdown.enable";

    public static final int CONTROLLED_SHUTDOWN_MAX_RETRIES = 3;
    public static final int CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS = 5000;
    public static final boolean CONTROLLED_SHUTDOWN_ENABLE = true;

    public static final String CONTROLLED_SHUTDOWN_MAX_RETRIES_DOC = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens";
    public static final String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DOC = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying.";
    public static final String CONTROLLED_SHUTDOWN_ENABLE_DOC = "Enable controlled shutdown of the server.";
}
