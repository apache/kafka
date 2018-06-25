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

package org.apache.kafka.clients.admin;


/*
 * This class implements the common APIs that are shared by Options classes for various AdminClient commands
 */
public abstract class AbstractOptions<T extends AbstractOptions> {

    protected Integer timeoutMs = null;

    /**
     * Set the request timeout in milliseconds for this operation or {@code null} if the default request timeout for the
     * AdminClient should be used.
     */
    @SuppressWarnings("unchecked")
    public T timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return (T) this;
    }

    /**
     * The request timeout in milliseconds for this operation or {@code null} if the default request timeout for the
     * AdminClient should be used.
     */
    public Integer timeoutMs() {
        return timeoutMs;
    }


}
