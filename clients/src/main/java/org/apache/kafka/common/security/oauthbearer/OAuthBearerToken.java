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

import java.util.Set;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The <code>b64token</code> value as defined in
 * <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section
 * 2.1</a> along with the token's specific scope and lifetime and principal
 * name.
 * <p>
 * A network request would be required to re-hydrate an opaque token, and that
 * could result in (for example) an {@code IOException}, but retrievers for
 * various attributes ({@link #scope()}, {@link #lifetimeMs()}, etc.) declare no
 * exceptions. Therefore, if a network request is required for any of these
 * retriever methods, that request could be performed at construction time so
 * that the various attributes can be reliably provided thereafter. For example,
 * a constructor might declare {@code throws IOException} in such a case.
 * Alternatively, the retrievers could throw unchecked exceptions.
 * <p>
 * This interface was introduced in 2.0.0 and, while it feels stable, it could
 * evolve. We will try to evolve the API in a compatible manner (easier now that
 * Java 7 and its lack of default methods doesn't have to be supported), but we
 * reserve the right to make breaking changes in minor releases, if necessary.
 * We will update the {@code InterfaceStability} annotation and this notice once
 * the API is considered stable.
 * 
 * @see <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC 6749
 *      Section 1.4</a> and
 *      <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750
 *      Section 2.1</a>
 */
@InterfaceStability.Evolving
public interface OAuthBearerToken {
    /**
     * The <code>b64token</code> value as defined in
     * <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section
     * 2.1</a>
     * 
     * @return <code>b64token</code> value as defined in
     *         <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750
     *         Section 2.1</a>
     */
    String value();

    /**
     * The token's scope of access, as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC 6749 Section
     * 1.4</a>
     * 
     * @return the token's (always non-null but potentially empty) scope of access,
     *         as per <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC
     *         6749 Section 1.4</a>. Note that all values in the returned set will
     *         be trimmed of preceding and trailing whitespace, and the result will
     *         never contain the empty string.
     */
    Set<String> scope();

    /**
     * The token's lifetime, expressed as the number of milliseconds since the
     * epoch, as per <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC
     * 6749 Section 1.4</a>
     * 
     * @return the token's lifetime, expressed as the number of milliseconds since
     *         the epoch, as per
     *         <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC 6749
     *         Section 1.4</a>.
     */
    long lifetimeMs();

    /**
     * The name of the principal to which this credential applies
     * 
     * @return the always non-null/non-empty principal name
     */
    String principalName();

    /**
     * When the credential became valid, in terms of the number of milliseconds
     * since the epoch, if known, otherwise null. An expiring credential may not
     * necessarily indicate when it was created -- just when it expires -- so we
     * need to support a null return value here.
     * 
     * @return the time when the credential became valid, in terms of the number of
     *         milliseconds since the epoch, if known, otherwise null
     */
    Long startTimeMs();
}
