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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.StateStore;

/**
 * Marker interface for the HEADERS-format outer-join {@link ListValueStore} changelog wrapper.
 * <p>
 * Used solely by {@code StateManagerUtil.converterForStore} to select the list-aware restore
 * {@link RecordConverters#rawListValueToHeadersListValue() converter}.
 * <p>
 * Note: this is intentionally NOT {@link org.apache.kafka.streams.state.HeadersBytesStore}. That
 * interface would make {@code WrappedStateStore.isHeadersAware} true and wrongly select
 * {@code rawValueToHeadersValue()}, which reconstructs a single {@code [headers][ts][value]} payload
 * and would corrupt the multi-element list blob used here.
 */
public interface HeadersAwareListValueStore extends StateStore {
}
