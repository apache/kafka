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

package org.apache.kafka.server.group.share;

import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;

public class InitializeShareGroupStateResult implements PersisterResult {
  private final short errorCode;

  private InitializeShareGroupStateResult(short errorCode) {
    this.errorCode = errorCode;
  }

  public short errorCode() {
    return errorCode;
  }

  public static InitializeShareGroupStateResult from(InitializeShareGroupStateResponseData data) {
    return new Builder()
        .setErrorCode(data.errorCode())
        .build();
  }

  public static class Builder {
    private short errorCode;

    public Builder setErrorCode(short errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    public InitializeShareGroupStateResult build() {
      return new InitializeShareGroupStateResult(errorCode);
    }
  }
}
