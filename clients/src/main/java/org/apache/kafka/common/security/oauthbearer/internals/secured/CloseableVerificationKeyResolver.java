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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.jose4j.keys.resolvers.VerificationKeyResolver;

/**
 * The {@link OAuthBearerValidatorCallbackHandler} uses a {@link VerificationKeyResolver} as
 * part of its validation of the incoming JWT. Some of the <code>VerificationKeyResolver</code>
 * implementations use resources like threads, connections, etc. that should be properly closed
 * when no longer needed. Since the <code>VerificationKeyResolver</code> interface itself doesn't
 * define a <code>close</code> method, we provide a means to do that here.
 *
 * @see OAuthBearerValidatorCallbackHandler
 * @see VerificationKeyResolver
 * @see Closeable
 */

public interface CloseableVerificationKeyResolver extends Initable, Closeable, VerificationKeyResolver {

    /**
     * Lifecycle method to perform a clean shutdown of the {@link VerificationKeyResolver}.
     * This must be performed by the caller to ensure the correct state, freeing up
     * and releasing any resources performed in {@link #init()}.
     *
     * @throws IOException Thrown on errors related to IO during closure
     */

    default void close() throws IOException {
        // This method left intentionally blank.
    }

}
