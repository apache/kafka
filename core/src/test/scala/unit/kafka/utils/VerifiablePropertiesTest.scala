/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.util.Properties
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class VerifiablePropertiesTest {

  @Test
  def testConstructorWithProperties(): Unit = {
    val props = new Properties()
    props.setProperty("test.key", "test.value")
    val vp = new VerifiableProperties(props)
    assertEquals("test.value", vp.getProperty("test.key"))
  }

  @Test
  def testDefaultConstructor(): Unit = {
    val vp = new VerifiableProperties()
    assertFalse(vp.containsKey("nonexistent"))
  }

  @Test
  def testCompanionObjectApply(): Unit = {
    val map = new java.util.HashMap[String, AnyRef]()
    map.put("key1", "value1")
    map.put("key2", "value2")
    val vp = VerifiableProperties(map)
    assertEquals("value1", vp.getProperty("key1"))
    assertEquals("value2", vp.getProperty("key2"))
  }

  @Test
  def testContainsKey(): Unit = {
    val props = new Properties()
    props.setProperty("existing.key", "value")
    val vp = new VerifiableProperties(props)
    assertTrue(vp.containsKey("existing.key"))
    assertFalse(vp.containsKey("nonexistent.key"))
  }

  @Test
  def testGetPropertyTrimsWhitespace(): Unit = {
    val props = new Properties()
    props.setProperty("key.1", "  value with spaces  ")
    val vp = new VerifiableProperties(props)
    assertEquals("value with spaces", vp.getProperty("key.1"))
  }

  @Test
  def testGetPropertyReturnsNull(): Unit = {
    val vp = new VerifiableProperties()
    assertNull(vp.getProperty("nonexistent.key"))
  }

  @Test
  def testGetIntRequired(): Unit = {
    val props = new Properties()
    props.setProperty("int.key", "42")
    val vp = new VerifiableProperties(props)
    assertEquals(42, vp.getInt("int.key"))
  }

  @Test
  def testGetIntRequiredThrowsWhenMissing(): Unit = {
    val vp = new VerifiableProperties()
    assertThrows(classOf[IllegalArgumentException], () => vp.getInt("missing.int"))
  }

  @Test
  def testGetIntWithDefault(): Unit = {
    val props = new Properties()
    props.setProperty("int.key", "100")
    val vp = new VerifiableProperties(props)
    
    assertEquals(100, vp.getInt("int.key", 50))
    assertEquals(50, vp.getInt("missing.int.key", 50))
  }

  @Test
  def testGetIntWithDefaultAcceptsMinMax(): Unit = {
    val vp = new VerifiableProperties()
    assertEquals(Int.MinValue, vp.getInt("missing", Int.MinValue))
    assertEquals(Int.MaxValue, vp.getInt("missing", Int.MaxValue))
  }

  @Test
  def testGetShortWithDefault(): Unit = {
    val props = new Properties()
    props.setProperty("short.key", "200")
    val vp = new VerifiableProperties(props)
    
    assertEquals(200.toShort, vp.getShort("short.key", 100.toShort))
    assertEquals(100.toShort, vp.getShort("missing.short.key", 100.toShort))
  }

  @Test
  def testGetShortWithDefaultAcceptsMinMax(): Unit = {
    val vp = new VerifiableProperties()
    assertEquals(Short.MinValue, vp.getShort("missing", Short.MinValue))
    assertEquals(Short.MaxValue, vp.getShort("missing", Short.MaxValue))
  }

  @Test
  def testGetLongRequired(): Unit = {
    val props = new Properties()
    props.setProperty("long.key", "9223372036854775807")
    val vp = new VerifiableProperties(props)
    assertEquals(9223372036854775807L, vp.getLong("long.key"))
  }

  @Test
  def testGetLongRequiredThrowsWhenMissing(): Unit = {
    val vp = new VerifiableProperties()
    assertThrows(classOf[IllegalArgumentException], () => vp.getLong("missing.long"))
  }

  @Test
  def testGetLongWithDefault(): Unit = {
    val props = new Properties()
    props.setProperty("long.key", "1000000")
    val vp = new VerifiableProperties(props)
    
    assertEquals(1000000L, vp.getLong("long.key", 500000L))
    assertEquals(500000L, vp.getLong("missing.long.key", 500000L))
  }

  @Test
  def testGetLongWithDefaultAcceptsMinMax(): Unit = {
    val vp = new VerifiableProperties()
    assertEquals(Long.MinValue, vp.getLong("missing", Long.MinValue))
    assertEquals(Long.MaxValue, vp.getLong("missing", Long.MaxValue))
  }

  @Test
  def testGetDoubleRequired(): Unit = {
    val props = new Properties()
    props.setProperty("double.key", "3.14159")
    val vp = new VerifiableProperties(props)
    assertEquals(3.14159, vp.getDouble("double.key"), 0.00001)
  }

  @Test
  def testGetDoubleRequiredThrowsWhenMissing(): Unit = {
    val vp = new VerifiableProperties()
    assertThrows(classOf[IllegalArgumentException], () => vp.getDouble("missing.double"))
  }

  @Test
  def testGetDoubleWithDefault(): Unit = {
    val props = new Properties()
    props.setProperty("double.key", "2.71828")
    val vp = new VerifiableProperties(props)
    
    assertEquals(2.71828, vp.getDouble("double.key", 1.0), 0.00001)
    assertEquals(1.0, vp.getDouble("missing.double.key", 1.0), 0.00001)
  }

  @Test
  def testGetBooleanRequired(): Unit = {
    val props = new Properties()
    props.setProperty("bool.true", "true")
    props.setProperty("bool.false", "false")
    val vp = new VerifiableProperties(props)
    
    assertTrue(vp.getBoolean("bool.true"))
    assertFalse(vp.getBoolean("bool.false"))
  }

  @Test
  def testGetBooleanRequiredThrowsWhenMissing(): Unit = {
    val vp = new VerifiableProperties()
    assertThrows(classOf[IllegalArgumentException], () => vp.getBoolean("missing.bool"))
  }

  @Test
  def testGetBooleanWithDefault(): Unit = {
    val props = new Properties()
    props.setProperty("bool.key", "true")
    val vp = new VerifiableProperties(props)
    
    assertTrue(vp.getBoolean("bool.key", false))
    assertFalse(vp.getBoolean("missing.bool.key", false))
    assertTrue(vp.getBoolean("missing.bool.key.2", true))
  }

  @Test
  def testGetBooleanWithDefaultThrowsOnInvalidValue(): Unit = {
    val props = new Properties()
    props.setProperty("invalid.bool", "yes")
    val vp = new VerifiableProperties(props)
    
    val exception = assertThrows(classOf[IllegalArgumentException], 
      () => vp.getBoolean("invalid.bool", false))
    assertTrue(exception.getMessage.contains("Unacceptable value"))
  }

  @Test
  def testGetStringRequired(): Unit = {
    val props = new Properties()
    props.setProperty("string.key", "test.value")
    val vp = new VerifiableProperties(props)
    assertEquals("test.value", vp.getString("string.key"))
  }

  @Test
  def testGetStringRequiredThrowsWhenMissing(): Unit = {
    val vp = new VerifiableProperties()
    val exception = assertThrows(classOf[IllegalArgumentException], 
      () => vp.getString("missing.string"))
    assertTrue(exception.getMessage.contains("Missing required property"))
  }

  @Test
  def testGetStringWithDefault(): Unit = {
    val props = new Properties()
    props.setProperty("string.key", "actual.value")
    val vp = new VerifiableProperties(props)
    
    assertEquals("actual.value", vp.getString("string.key", "default.value"))
    assertEquals("default.value", vp.getString("missing.string.key", "default.value"))
  }

  @Test
  def testGetMapWithValidEntries(): Unit = {
    val props = new Properties()
    props.setProperty("map.key", "k1:v1, k2:v2, k3:v3")
    val vp = new VerifiableProperties(props)
    
    val map = vp.getMap("map.key")
    assertEquals(3, map.size)
    assertEquals("v1", map("k1"))
    assertEquals("v2", map("k2"))
    assertEquals("v3", map("k3"))
  }

  @Test
  def testGetMapWithEmptyString(): Unit = {
    val vp = new VerifiableProperties()
    val map = vp.getMap("nonexistent.map")
    assertTrue(map.isEmpty)
  }

  @Test
  def testGetMapWithValidationFunction(): Unit = {
    val props = new Properties()
    props.setProperty("validated.map", "k1:valid1, k2:valid2")
    val vp = new VerifiableProperties(props)
    
    val map = vp.getMap("validated.map", value => value.startsWith("valid"))
    assertEquals(2, map.size)
  }

  @Test
  def testGetMapWithValidationFunctionThrowsOnInvalidValue(): Unit = {
    val props = new Properties()
    props.setProperty("invalid.map", "k1:valid, k2:invalid")
    val vp = new VerifiableProperties(props)
    
    val exception = assertThrows(classOf[IllegalArgumentException], 
      () => vp.getMap("invalid.map", value => value.startsWith("valid")))
    assertTrue(exception.getMessage.contains("Invalid entry"))
  }

  @Test
  def testGetMapWithMalformedEntries(): Unit = {
    val props = new Properties()
    props.setProperty("malformed.map", "k1:v1, malformed, k2:v2")
    val vp = new VerifiableProperties(props)
    
    val exception = assertThrows(classOf[IllegalArgumentException], 
      () => vp.getMap("malformed.map"))
    assertTrue(exception.getMessage.contains("Error parsing configuration property"))
  }

  @Test
  def testToString(): Unit = {
    val props = new Properties()
    props.setProperty("key1", "value1")
    props.setProperty("key2", "value2")
    val vp = new VerifiableProperties(props)
    
    val result = vp.toString
    assertNotNull(result)
    
    // Properties.toString() returns format: {key1=value1, key2=value2}
    // Order is not guaranteed since Properties extends Hashtable
    assertTrue(result.startsWith("{"), "Should start with {")
    assertTrue(result.endsWith("}"), "Should end with }")
    
    assertTrue(result.contains("key1=value1"), "Should contain key1=value1")
    assertTrue(result.contains("key2=value2"), "Should contain key2=value2")
  }

  @Test
  def testGetIntInvalidFormatThrowsException(): Unit = {
    val props = new Properties()
    props.setProperty("invalid.int", "not_a_number")
    val vp = new VerifiableProperties(props)
    
    assertThrows(classOf[NumberFormatException], () => vp.getInt("invalid.int", 0))
  }

  @Test
  def testGetLongInvalidFormatThrowsException(): Unit = {
    val props = new Properties()
    props.setProperty("invalid.long", "not_a_number")
    val vp = new VerifiableProperties(props)
    
    assertThrows(classOf[NumberFormatException], () => vp.getLong("invalid.long", 0L))
  }

  @Test
  def testGetDoubleInvalidFormatThrowsException(): Unit = {
    val props = new Properties()
    props.setProperty("invalid.double", "not_a_number")
    val vp = new VerifiableProperties(props)
    
    assertThrows(classOf[NumberFormatException], () => vp.getDouble("invalid.double", 0.0))
  }

  @Test
  def testGetShortInvalidFormatThrowsException(): Unit = {
    val props = new Properties()
    props.setProperty("invalid.short", "not_a_number")
    val vp = new VerifiableProperties(props)
    
    assertThrows(classOf[NumberFormatException], () => vp.getShort("invalid.short", 0.toShort))
  }

  @Test
  def testGetBooleanInvalidFormatThrowsException(): Unit = {
    val props = new Properties()
    props.setProperty("invalid.bool", "not_true_or_false")
    val vp = new VerifiableProperties(props)
    
    assertThrows(classOf[IllegalArgumentException], () => vp.getBoolean("invalid.bool"))
  }

  @Test
  def testGetPropertyEmptyString(): Unit = {
    val props = new Properties()
    props.setProperty("empty.key", "")
    val vp = new VerifiableProperties(props)
    
    assertEquals("", vp.getProperty("empty.key"))
  }

  @Test
  def testGetStringEmptyString(): Unit = {
    val props = new Properties()
    props.setProperty("empty.string", "")
    val vp = new VerifiableProperties(props)
    
    assertEquals("", vp.getString("empty.string"))
  }

  @Test
  def testGetMapWithSingleEntry(): Unit = {
    val props = new Properties()
    props.setProperty("single.map", "key:value")
    val vp = new VerifiableProperties(props)
    
    val map = vp.getMap("single.map")
    assertEquals(1, map.size)
    assertEquals("value", map("key"))
  }

  @Test
  def testGetMapWithComplexValues(): Unit = {
    val props = new Properties()
    props.setProperty("complex.map", "k1:v1 with spaces, k2:v2-with-dashes, k3:v3_with_underscores")
    val vp = new VerifiableProperties(props)
    
    val map = vp.getMap("complex.map")
    assertEquals(3, map.size)
    assertEquals("v1 with spaces", map("k1"))
    assertEquals("v2-with-dashes", map("k2"))
    assertEquals("v3_with_underscores", map("k3"))
  }

  @Test
  def testPropertiesReferenceSetTracking(): Unit = {
    val props = new Properties()
    props.setProperty("key1", "value1")
    props.setProperty("key2", "value2")
    val vp = new VerifiableProperties(props)
    
    // Access key1
    vp.getString("key1", "default")
    
    // Verify tracks which properties were accessed
    // This is indirectly tested through verify() method behavior
    // as it should not throw any exceptions
    vp.verify()
  }

  @Test
  def testGetIntNegativeValue(): Unit = {
    val props = new Properties()
    props.setProperty("negative.int", "-42")
    val vp = new VerifiableProperties(props)
    assertEquals(-42, vp.getInt("negative.int", 0))
  }

  @Test
  def testGetLongNegativeValue(): Unit = {
    val props = new Properties()
    props.setProperty("negative.long", "-9223372036854775807")
    val vp = new VerifiableProperties(props)
    assertEquals(-9223372036854775807L, vp.getLong("negative.long", 0L))
  }

  @Test
  def testGetDoubleNegativeValue(): Unit = {
    val props = new Properties()
    props.setProperty("negative.double", "-3.14159")
    val vp = new VerifiableProperties(props)
    assertEquals(-3.14159, vp.getDouble("negative.double", 0.0), 0.00001)
  }
}

