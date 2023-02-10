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

import javax.security.auth.callback.Callback;
import org.apache.kafka.common.KafkaException;

/**
 * ValidateException is thrown in cases where a JWT access token cannot be determined to be
 * valid for one reason or another. It is intended to be used when errors arise within the
 * processing of a {@link javax.security.auth.callback.CallbackHandler#handle(Callback[])}.
 * This error, however, is not thrown from that method directly.
 *
 * @see AccessTokenValidator#validate(String)
 */

public class ValidateException extends KafkaException {

    public ValidateException(String message) {
        super(message);
    }

    public ValidateException(Throwable cause) {
        super(cause);
    }

    public ValidateException(String message, Throwable cause) {
        super(message, cause);
    }

}
