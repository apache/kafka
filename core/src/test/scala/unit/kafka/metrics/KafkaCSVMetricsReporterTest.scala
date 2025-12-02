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

package kafka.metrics

import kafka.utils.{TestUtils, VerifiableProperties}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.File
import java.nio.file.Files
import java.util.Properties
import scala.jdk.CollectionConverters._

class KafkaCSVMetricsReporterTest {

  private var testDir: File = _
  private var metricsPath: File = _
  private var reporter: KafkaMetricsReporter = _
  private var reporterMBean: KafkaMetricsReporterMBean = _

  @BeforeEach
  def setUp(): Unit = {
    testDir = TestUtils.tempDir()
    metricsPath = new File(testDir, "csv_metrics")
  }

  @AfterEach
  def tearDown(): Unit = {
    if (reporterMBean != null) {
      try {
        reporterMBean.stopReporter()
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
    if (testDir != null && testDir.exists()) {
      Utils.delete(testDir)
    }
    // Clean up default directory if created
    val defaultDir = new File("kafka_metrics")
    if (defaultDir.exists()) {
      Utils.delete(defaultDir)
    }
  }

  @Test
  def testReporterCreationAndMBeanName(): Unit = {
    reporter = createCSVReporter(enabled = false)
    reporterMBean = reporter.asInstanceOf[KafkaMetricsReporterMBean]
    
    assertEquals("kafka:type=kafka.metrics.KafkaCSVMetricsReporter", 
      reporterMBean.getMBeanName)
  }

  @Test
  def testDirectoryCreation(): Unit = {
    reporter = createCSVReporter(enabled = false)
    
    assertTrue(metricsPath.exists(), "CSV metrics directory should be created")
    assertTrue(metricsPath.isDirectory, "CSV metrics path should be a directory")
    assertEquals(0, metricsPath.list().length, "Directory should be empty initially")
  }

  @Test
  def testDirectoryCreationWithDefaultPath(): Unit = {
    val props = new Properties()
    props.setProperty("kafka.csv.metrics.reporter.enabled", "false")
    
    reporter = createCSVReporterWithProps(props)
    
    val defaultDir = new File("kafka_metrics")
    assertTrue(defaultDir.exists(), "Default CSV metrics directory should be created")
    assertTrue(defaultDir.isDirectory, "Default path should be a directory")
  }

  @Test
  def testOldDirectoryIsDeleted(): Unit = {
    // Create directory with some old files
    Files.createDirectories(metricsPath.toPath)
    val oldFile = new File(metricsPath, "old_metric.csv")
    Files.write(oldFile.toPath, "old data".getBytes)
    assertTrue(oldFile.exists(), "Old file should exist")
    
    reporter = createCSVReporter(enabled = false)
    
    assertTrue(metricsPath.exists(), "Metrics directory should exist")
    assertFalse(oldFile.exists(), "Old files should be deleted")
    assertEquals(0, metricsPath.list().length, "Directory should be empty after init")
  }

  @Test
  def testReporterStartsWhenEnabled(): Unit = {
    // Create a test metric
    val metricsGroup = new KafkaMetricsGroup("kafka.test", "CSVTest")
    metricsGroup.newGauge("TestGauge", () => 42)
    
    try {
      reporter = createCSVReporter(enabled = true, pollingInterval = 1)
      reporterMBean = reporter.asInstanceOf[KafkaMetricsReporterMBean]
      
      // Wait for at least one polling cycle
      Thread.sleep(2000)
      
      // Stop the reporter
      reporterMBean.stopReporter()
      
      // Check that CSV files were created
      val csvFiles = metricsPath.listFiles().filter(_.getName.endsWith(".csv"))
      assertTrue(csvFiles.length > 0, "CSV files should be created when reporter is enabled")
      
      // Verify at least one file has content
      val filesWithContent = csvFiles.filter(f => Files.size(f.toPath) > 0)
      assertTrue(filesWithContent.length > 0, "At least one CSV file should have content")
    } finally {
      metricsGroup.removeMetric("TestGauge")
    }
  }

  @Test
  def testReporterDoesNotStartWhenDisabled(): Unit = {
    reporter = createCSVReporter(enabled = false)
    
    // Wait a bit
    Thread.sleep(1000)
    
    // No CSV files should be created
    val csvFiles = metricsPath.listFiles().filter(_.getName.endsWith(".csv"))
    assertEquals(0, csvFiles.length, "No CSV files should be created when disabled")
  }

  @Test
  def testManualStart(): Unit = {
    reporter = createCSVReporter(enabled = false)
    reporterMBean = reporter.asInstanceOf[KafkaMetricsReporterMBean]
    
    // Verify reporter can be manually started without error
    reporterMBean.startReporter(1)
    
    // Wait for at least one polling cycle
    Thread.sleep(1500)
    
    reporterMBean.stopReporter()
    
    val csvFiles = metricsPath.listFiles()
    assertNotNull(csvFiles, "Metrics directory should exist")
  }

  @Test
  def testStopAndRestart(): Unit = {
    // Create a test metric
    val metricsGroup = new KafkaMetricsGroup("kafka.test", "RestartTest")
    metricsGroup.newGauge("RestartGauge", () => 200)
    
    try {
      reporter = createCSVReporter(enabled = true, pollingInterval = 1)
      reporterMBean = reporter.asInstanceOf[KafkaMetricsReporterMBean]
      
      Thread.sleep(1500)
      
      // Stop the reporter
      reporterMBean.stopReporter()
      
      // Verify files were created
      val filesAfterStop = metricsPath.listFiles().filter(_.getName.endsWith(".csv"))
      val countAfterStop = filesAfterStop.length
      assertTrue(countAfterStop > 0, "Files should exist after stop")
      
      // Restart
      reporterMBean.startReporter(1)
      Thread.sleep(1500)
      reporterMBean.stopReporter()
      
      // Should still have files
      val filesAfterRestart = metricsPath.listFiles().filter(_.getName.endsWith(".csv"))
      assertTrue(filesAfterRestart.length > 0, "Files should exist after restart")
    } finally {
      metricsGroup.removeMetric("RestartGauge")
    }
  }

  @Test
  def testMultipleStopsAreSafe(): Unit = {
    reporter = createCSVReporter(enabled = true, pollingInterval = 10)
    reporterMBean = reporter.asInstanceOf[KafkaMetricsReporterMBean]
    
    // Multiple stops should not throw
    reporterMBean.stopReporter()
    reporterMBean.stopReporter()
    reporterMBean.stopReporter()
  }

  @Test
  def testMultipleStartsAreSafe(): Unit = {
    reporter = createCSVReporter(enabled = false)
    reporterMBean = reporter.asInstanceOf[KafkaMetricsReporterMBean]
    
    // Multiple starts should not throw
    reporterMBean.startReporter(10)
    reporterMBean.startReporter(10)
    reporterMBean.startReporter(10)
    
    reporterMBean.stopReporter()
  }

  @Test
  def testCsvFileFormat(): Unit = {
    // Create a test metric
    val metricsGroup = new KafkaMetricsGroup("kafka.test", "FormatTest")
    metricsGroup.newGauge("FormatGauge", () => 12345)
    
    try {
      reporter = createCSVReporter(enabled = true, pollingInterval = 1)
      reporterMBean = reporter.asInstanceOf[KafkaMetricsReporterMBean]
      
      // Wait for metric to be written
      Thread.sleep(2000)
      
      reporterMBean.stopReporter()
      
      // Find CSV files
      val csvFiles = metricsPath.listFiles().filter(_.getName.endsWith(".csv"))
      assertTrue(csvFiles.length > 0, "CSV files should be created")
      
      // Check that files contain properly formatted CSV data
      csvFiles.foreach { file =>
        val lines = Files.readAllLines(file.toPath).asScala
        if (lines.nonEmpty) {
          val header = lines.head
          // Yammer CSV reporter headers can start with "# time" or "t"
          val hasValidHeader = header.contains("time") || header.startsWith("t")
          assertTrue(hasValidHeader, s"CSV header should contain 'time': $header")
          
          // If there's data, check it's formatted correctly
          if (lines.size > 1) {
            // Find first non-comment line
            val dataLine = lines.drop(1).find(!_.startsWith("#")).getOrElse("")
            if (dataLine.nonEmpty) {
              assertTrue(dataLine.contains(","), "Data line should contain commas")
              // First value should be a timestamp (numeric)
              val firstValue = dataLine.split(",")(0).trim
              assertTrue(firstValue.nonEmpty && firstValue.forall(c => c.isDigit || c == '.'), 
                s"First column should be timestamp (numeric): $firstValue")
            }
          }
        }
      }
    } finally {
      metricsGroup.removeMetric("FormatGauge")
    }
  }

  @Test
  def testCustomPollingInterval(): Unit = {
    val props = new Properties()
    props.setProperty("kafka.csv.metrics.dir", metricsPath.getAbsolutePath)
    props.setProperty("kafka.csv.metrics.reporter.enabled", "false")
    props.setProperty("kafka.metrics.polling.interval.secs", "5")
    
    reporter = createCSVReporterWithProps(props)
    
    // Reporter should be created successfully with custom polling interval
    assertNotNull(reporter)
  }

  /**
   * Helper method to create a CSV reporter with specified configuration
   */
  private def createCSVReporter(enabled: Boolean, pollingInterval: Int = 10): KafkaMetricsReporter = {
    val props = new Properties()
    props.setProperty("kafka.csv.metrics.dir", metricsPath.getAbsolutePath)
    props.setProperty("kafka.csv.metrics.reporter.enabled", enabled.toString)
    props.setProperty("kafka.metrics.polling.interval.secs", pollingInterval.toString)
    
    createCSVReporterWithProps(props)
  }

  /**
   * Helper method to create a CSV reporter with custom properties
   */
  private def createCSVReporterWithProps(props: Properties): KafkaMetricsReporter = {
    val verifiableProps = new VerifiableProperties(props)
    
    // Use reflection to instantiate the private KafkaCSVMetricsReporter class
    val reporterClass = Class.forName("kafka.metrics.KafkaCSVMetricsReporter")
    val constructor = reporterClass.getDeclaredConstructor()
    constructor.setAccessible(true)
    val reporterInstance = constructor.newInstance().asInstanceOf[KafkaMetricsReporter]
    
    // Initialize the reporter
    reporterInstance.init(verifiableProps)
    
    reporterInstance
  }
}
