/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class PhoneHomeResponse {

  private final String confluentPlatformVersion;
  private final String informationForUser;

  @JsonCreator
  public PhoneHomeResponse(
      @JsonProperty("confluentPlatformVersion") String confluentPlatformVersion,
      @JsonProperty("informationForUser") String informationForUser
  ) {
    this.confluentPlatformVersion = confluentPlatformVersion;
    this.informationForUser = informationForUser;
  }

  /**
   * The latest available version of the Confluent Platform or null.
   */
  public String getConfluentPlatformVersion() {
    return confluentPlatformVersion;
  }

  /**
   * Short text with information to the user or null.
   */
  public String getInformationForUser() {
    return informationForUser;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PhoneHomeResponse)) {
      return false;
    }
    PhoneHomeResponse that = (PhoneHomeResponse) o;
    return Objects.equals(getConfluentPlatformVersion(), that.getConfluentPlatformVersion())
           && Objects.equals(getInformationForUser(), that.getInformationForUser());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getConfluentPlatformVersion(), getInformationForUser());
  }
}
