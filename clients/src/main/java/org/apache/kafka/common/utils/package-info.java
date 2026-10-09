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
/**
 * Provides utilities for Kafka server and clients.
 *
 * <p>This package contains the public API class {@link org.apache.kafka.common.utils.Bytes}, which was made
 * part of the public API via KIP-1247.
 *
 * <p>Other classes in this package, including {@link org.apache.kafka.common.utils.Time} and {@link org.apache.kafka.common.utils.Timer},
 * are currently exposed through Kafka APIs but are not yet officially designated
 * as public API. A future KIP will address their public API status and design.
 *
 * <p>The remaining classes in this package are internal utilities and not part of
 * the supported Kafka API; their implementation may change without warning
 * between releases.
 */
package org.apache.kafka.common.utils;