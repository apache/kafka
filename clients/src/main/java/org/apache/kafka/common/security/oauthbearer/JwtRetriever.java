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
package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.security.oauthbearer.internals.secured.JwtHttpClient;

import javax.security.auth.spi.LoginModule;

/**
 * An implementation of <code>JwtRetriever</code> is the means by which the login module will
 * retrieve an OAuth JWT that is used to authorize with a broker. The implementation may
 * involve authentication to one or more remote systems, or it can be as simple as loading the contents
 * from a file or configuration setting.
 *
 * <i>Retrieval</i> of a token is a separate concern from <i>validation</i>. <code>JwtRetriever</code>
 * implementations should not validate the integrity of the JWT, but should rely on the companion
 * {@link JwtValidator} for that task.
 *
 * @see ClientCredentialsJwtRetriever
 * @see DefaultJwtRetriever
 * @see FileJwtRetriever
 * @see JwtHttpClient
 * @see JwtBearerJwtRetriever
 */
public interface JwtRetriever extends OAuthBearerConfigurable {

    /**
     * <p>
     * Retrieves a JWT access token in its serialized three-part form. The implementation is free to
     * determine how it should be retrieved but should not perform validation on the result.
     * </p>
     *
     * <p>
     * <b>Note</b>: This is a blocking function and callers should be aware that the
     * implementation may be communicating over a network, with the file system, coordinating
     * threads, etc. The facility in the {@link LoginModule} from which this is ultimately called does
     * not provide an asynchronous approach.
     * </p>
     *
     * @return Non-<code>null</code> JWT access token string
     *
     * @throws JwtRetrieverException Thrown on errors related to retrieval
     */
    String retrieve() throws JwtRetrieverException;

    /**
     * Closes any resources used by this implementation. The default implementation of
     * this method is a no op, for convenience to implementors.
     */
    @Override
    default void close() {
        // Do nothing...
    }
}
