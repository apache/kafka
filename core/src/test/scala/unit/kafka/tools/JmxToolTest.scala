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
package kafka.tools

import java.lang.management.ManagementFactory
import java.util.Locale

import javax.management.{MBeanServer, ObjectName}
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.KafkaThread
import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert._

trait JmxToolTestColorMBean {
  def getColor: String
  def getUpperColor: String
}

class JmxToolTestColor(color: String) extends JmxToolTestColorMBean {
  override def getColor: String = color
  override def getUpperColor: String = color.toUpperCase(Locale.ROOT)
}

trait JmxToolTestNumMBean {
  def getNum: Int
}

class JmxToolTestNum(num: Int) extends JmxToolTestNumMBean {
  override def getNum: Int = num
}

object JmxToolTest {
  private val server: MBeanServer = ManagementFactory.getPlatformMBeanServer
  private val magenta = new ObjectName("kafka.test:type=Integration,color=M")
  private val yellow = new ObjectName("kafka.test:type=Integration,color=Y")
  private val zero = new ObjectName("kafka.test:type=Integration,num=zero")

  @BeforeClass
  def before(): Unit = {
    server.registerMBean(new JmxToolTestColor("magenta"), magenta)
    server.registerMBean(new JmxToolTestColor("yellow"), yellow)
    server.registerMBean(new JmxToolTestNum(0), zero)
  }

  @AfterClass
  def after(): Unit = {
    server.unregisterMBean(magenta)
    server.unregisterMBean(yellow)
    server.unregisterMBean(zero)
  }
}

class JmxToolTest {
  import JmxToolTest._

  private def assertToolError(args: Array[String], expectPrintUsage: Boolean, expectedMessage: String): (String, String) = {
    TestUtils.grabConsoleOutputAndError {
      val result = JmxTool.execute(JmxTool.ToolOptions(args), server, 200)
      assertTrue(result.isDefined)
      assertEquals(expectPrintUsage, result.get._1)
      assertEquals(expectedMessage, result.get._2)
    }
  }

  private def assertToolSuccess(args: Array[String]): (String, String) = {
    TestUtils.grabConsoleOutputAndError {
      val result = JmxTool.execute(JmxTool.ToolOptions(args), server)
      assertTrue(result.isEmpty)
    }
  }

  @Test
  def testObjectName(): Unit = {
    val (out, error) = assertToolSuccess(Array(
      "--one-time", "true",
      "--object-name", "kafka.test:type=Integration,color=M"))
    assertTrue(error.isEmpty)
    assertTrue(out.contains("magenta"))
    assertTrue(out.contains("MAGENTA"))
  }

  @Test
  def testAttributes(): Unit = {
    // That --attributes excludes non-selected attrs
    var outErr = assertToolSuccess(Array(
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=M",
        "--attributes", "Color"))
    assertTrue(outErr._2.isEmpty)
    assertTrue(outErr._1.contains("magenta"))
    assertFalse(outErr._1.contains("MAGENTA"))

    // That --attributes narrows the matching mbeans to those with the attribute
    outErr = assertToolSuccess(Array(
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,*",
        "--attributes", "Color"))
    assertTrue(outErr._2.isEmpty)
    assertTrue(outErr._1.contains("magenta"))
    assertFalse(outErr._1.contains("MAGENTA"))
    assertFalse(outErr._1.contains("kafka.test:type=Integration,num=zero:Num"))
    assertFalse(outErr._1.contains(",0"))

    // That without --attributes we don't get the narrowing
    outErr = assertToolSuccess(Array(
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,*"))
    assertTrue(outErr._2.isEmpty)
    assertTrue(outErr._1.contains("magenta"))
    assertTrue(outErr._1.contains("MAGENTA"))
    assertTrue(outErr._1.contains("kafka.test:type=Integration,num=zero:Num"))
    assertTrue(outErr._1.contains(",0"))
  }

  @Test
  def testNoMatchedAttributes(): Unit = {
    val (out, err) = assertToolError(Array(
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=M",
        "--attributes", "Nonexistent"),
      true, "No matched attributes for the queried objects kafka.test:type=Integration,color=M.")
    assertTrue(err.isEmpty)
    assertTrue(out.isEmpty)
  }

  @Test
  def testPattern(): Unit = {
    val (out, error) = assertToolSuccess(Array(
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=*"))
    assertTrue(error.isEmpty)
    assertTrue(out.contains("magenta"))
    assertTrue(out.contains("MAGENTA"))
    assertTrue(out.contains("yellow"))
    assertTrue(out.contains("YELLOW"))
  }

  @Test
  def testObjectNameNoWaitWithTimeout(): Unit = {
    val (out, error) = assertToolError(Array(
        "--one-time", "true",
        "--object-name", "this.object:type=DoesNot,name=Exist",
        "--object-name", "kafka.test:type=Integration,color=M"),
      false,"Exiting.")
    assertEquals("Could not find any objects names matching this.object:type=DoesNot,name=Exist.", error.trim)
    assertTrue(out.isEmpty)
  }

  @Test
  def testPatternNoWaitWithTimeout(): Unit = {
    val (out, error) = assertToolError(Array(
        "--one-time", "true",
        "--object-name", "this.object:type=DoesNot,name=*",
        "--object-name", "kafka.test:type=Integration,color=M"),
      false, "Exiting.")
    assertEquals("Could not find any objects names matching this.object:type=DoesNot,name=*.", error.trim)
    assertTrue(out.isEmpty)
  }

  @Test
  def testObjectNameWaitWithTimeout(): Unit = {
    val (out, error) = assertToolError(Array(
      "--wait",
      "--one-time", "true",
      "--object-name", "this.object:type=DoesNot,name=Exist",
      "--object-name", "kafka.test:type=Integration,color=M"),
      false, "Exiting.")
    assertTrue(error.trim.endsWith("Could not find any objects names matching this.object:type=DoesNot,name=Exist after 200 ms."))
    assertTrue(out.isEmpty)
  }


  @Test
  def testPatternWaitWithTimeout(): Unit = {
    val (out, error) = assertToolError(Array(
        "--wait",
        "--one-time", "true",
        "--object-name", "this.object:type=DoesNot,name=*",
        "--object-name", "kafka.test:type=Integration,color=M"),
      false, "Exiting.")
    assertTrue(error.trim.endsWith("Could not find any objects names matching this.object:type=DoesNot,name=* after 200 ms."))
    assertTrue(out.isEmpty)
  }

  @Test
  def testObjectNameWaitEventuallyFound(): Unit = {
    val name = new ObjectName("kafka.test:type=Integration,color=C")
    val thread = KafkaThread.daemon(classOf[JmxToolTest].getName, () => {
      Thread.sleep(500)
      server.registerMBean(new JmxToolTestColor("cyan"), name)
    })
    thread.start()
    try {
      val (out, _) = assertToolSuccess(Array(
          "--wait",
          "--one-time", "true",
          "--object-name", "kafka.test:type=Integration,color=C",
          "--object-name", "kafka.test:type=Integration,color=M"))
      assertTrue(out.contains("cyan"))
      assertTrue(out.contains("CYAN"))
      assertTrue(out.contains("magenta"))
      assertTrue(out.contains("MAGENTA"))

    } finally {
      thread.join()
      server.unregisterMBean(name)
    }
  }

  @Test
  def testPatternWaitEventuallyFound(): Unit = {

    val name = new ObjectName("kafka.test:type=Integration,color=C")
    val thread = KafkaThread.daemon(classOf[JmxToolTest].getName, () => {
      Thread.sleep(500)
      server.registerMBean(new JmxToolTestColor("cyan"), name)
    })
    thread.start()
    try {
      val (out, _) = assertToolSuccess(Array(
          "--wait",
          "--one-time", "true",
          "--object-name", "kafka.test:type=*,color=M",
          "--object-name", "kafka.test:type=*,color=C"))
      assertTrue(out.contains("cyan"))
      assertTrue(out.contains("CYAN"))
      assertTrue(out.contains("magenta"))
      assertTrue(out.contains("MAGENTA"))
    } finally {
      thread.join()
      server.unregisterMBean(name)
    }
  }

  @Test
  def testOriginalReportFormat(): Unit = {
    val (out, error) = assertToolSuccess(Array(
        "--report-format", "original",
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=M"))
    assertTrue(error.isEmpty)
    assertTrue(out.trim.matches("\"time\",\"kafka.test:type=Integration,color=M:Color\",\"kafka.test:type=Integration,color=M:UpperColor\"" +
      "[\n\r]+[0-9]+,magenta,MAGENTA"))
  }

  @Test
  def testCsvReportFormat(): Unit = {
    val (out, error) = assertToolSuccess(Array(
        "--report-format", "csv",
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=M"))
    assertTrue(error.isEmpty)
    assertTrue(out.trim.matches("time,\"[0-9]+\"[\n\r]+" +
      "kafka.test:type=Integration,color=M:Color,\"magenta\"[\n\r]+" +
      "kafka.test:type=Integration,color=M:UpperColor,\"MAGENTA\""))
  }

  @Test
  def testTsvReportFormat(): Unit = {
    val (out, error) = assertToolSuccess(Array(
        "--report-format", "tsv",
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=M"))
    assertTrue(error.isEmpty)
    assertTrue(out.trim.matches("time\t[0-9]+[\n\r]+" +
      "kafka.test:type=Integration,color=M:Color\tmagenta[\n\r]+" +
      "kafka.test:type=Integration,color=M:UpperColor\tMAGENTA"))
  }

  @Test
  def testPropertiesReportFormat(): Unit = {
    val (out, error) = assertToolSuccess(Array(
        "--report-format", "properties",
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=M"))
    assertTrue(error.isEmpty)
    assertTrue(out.trim.matches("time=[0-9]+[\n\r]+" +
      "kafka.test:type=Integration,color=M:Color=magenta[\n\r]+" +
      "kafka.test:type=Integration,color=M:UpperColor=MAGENTA"))
  }

  @Test
  def testDateFormat(): Unit = {
    val (out, error) = assertToolSuccess(Array(
        "--date-format", "yyyy-MM-dd HH:mm:ss",
        "--one-time", "true",
        "--object-name", "kafka.test:type=Integration,color=M"))
    assertTrue(error.isEmpty)
    assertTrue(out.trim.matches("\"time\",\"kafka.test:type=Integration,color=M:Color\",\"kafka.test:type=Integration,color=M:UpperColor\"[\n\r]+" +
      "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},magenta,MAGENTA"))
  }

}
