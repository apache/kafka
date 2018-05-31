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
package org.apache.kafka.common.security.oauthbearer.internal.expiring;

/**
 * A credential that expires and that can potentially be refreshed
 * 
 * @see ExpiringCredentialRefreshingLogin
 */
public interface ExpiringCredential {
    /**
     * The name of the principal to which this credential applies (used only for
     * logging)
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

    /**
     * When the credential expires, in terms of the number of milliseconds since the
     * epoch. All expiring credentials by definition must indicate their expiration
     * time -- thus, unlike other methods, we do not support a null return value
     * here.
     * 
     * @return the time when the credential expires, in terms of the number of
     *         milliseconds since the epoch
     */
    long expireTimeMs();

    /**
     * The point after which the credential can no longer be refreshed, in terms of
     * the number of milliseconds since the epoch, if any, otherwise null. Some
     * expiring credentials can be refreshed over and over again without limit, so
     * we support a null return value here.
     * 
     * @return the point after which the credential can no longer be refreshed, in
     *         terms of the number of milliseconds since the epoch, if any,
     *         otherwise null
     */
    Long absoluteLastRefreshTimeMs();
}
