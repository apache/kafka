/**
 * Copyright 2019 Confluent Inc.
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

package io.confluent.support.metrics.utils;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringUtilsTest {
  @Test
  public void testIsBlank() throws IOException {
    assertTrue(StringUtils.isNullOrBlank(null));
    assertTrue(StringUtils.isNullOrBlank(""));
    assertTrue(StringUtils.isNullOrBlank("  "));
    assertFalse(StringUtils.isNullOrBlank(" a "));
    assertFalse(StringUtils.isNullOrBlank("abc"));
  }

  @Test
  public void testIsNullOrEmpty() throws IOException {
    assertTrue(StringUtils.isNullOrEmpty(null));
    assertTrue(StringUtils.isNullOrEmpty(""));
    assertFalse(StringUtils.isNullOrEmpty(" "));
    assertFalse(StringUtils.isNullOrEmpty("abc"));
    assertFalse(StringUtils.isNullOrEmpty(" a"));
  }
}
