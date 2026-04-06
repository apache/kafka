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

package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import java.io.Closeable;
import java.util.function.Supplier;

/**
 * A {@link Supplier} that also implements {@link Closeable}, allowing the supplier to hold
 * resources that need to be properly cleaned up.
 *
 * <p>
 * This interface is particularly useful when a supplier needs to manage resources like file
 * handles, network connections, or cryptographic resources that must be released when no
 * longer needed.
 * </p>
 *
 * @param <T> The type of object supplied by this supplier
 */
public interface CloseableSupplier<T> extends Supplier<T>, Closeable {
}
