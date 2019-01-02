package kafka.utils

import java.util.Properties

import joptsimple.{OptionParser, OptionSpec}
import org.junit.Assert.{assertEquals, assertNull}
import org.junit.Test

class ConfigUtilsTest {
  val props = new Properties()
  val parser = new OptionParser(false)
  var stringOpt : OptionSpec[String] = _
  var intOpt : OptionSpec[java.lang.Integer] = _
  var stringOptOptionalArg : OptionSpec[String] = _
  var intOptOptionalArg : OptionSpec[java.lang.Integer] = _
  var stringOptOptionalArgNoDefault : OptionSpec[String] = _
  var intOptOptionalArgNoDefault : OptionSpec[java.lang.Integer] = _

  def setUpOptions(): Unit = {
    stringOpt = parser.accepts("str")
      .withRequiredArg
      .ofType(classOf[String])
      .defaultsTo("default-string")
    intOpt = parser.accepts("int")
      .withRequiredArg()
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    stringOptOptionalArg = parser.accepts("str-opt")
      .withOptionalArg
      .ofType(classOf[String])
      .defaultsTo("default-string-2")
    intOptOptionalArg = parser.accepts("int-opt")
      .withOptionalArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    stringOptOptionalArgNoDefault = parser.accepts("str-opt-nodef")
      .withOptionalArg
      .ofType(classOf[String])
    intOptOptionalArgNoDefault = parser.accepts("int-opt-nodef")
      .withOptionalArg
      .ofType(classOf[java.lang.Integer])
  }

  @Test
  def testMaybeMergeOptionsOverwriteExisting(): Unit = {
    setUpOptions()

    props.put("skey", "existing-string")
    props.put("ikey", "300")
    props.put("sokey", "existing-string-2")
    props.put("iokey", "400")
    props.put("sondkey", "existing-string-3")
    props.put("iondkey", "500")

    val options = parser.parse(
      "--str", "some-string",
      "--int", "600",
      "--str-opt", "some-string-2",
      "--int-opt", "700",
      "--str-opt-nodef", "some-string-3",
      "--int-opt-nodef", "800",
    )

    ConfigUtils.maybeMergeOptions(props, "skey", options, stringOpt)
    ConfigUtils.maybeMergeOptions(props, "ikey", options, intOpt)
    ConfigUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    ConfigUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("some-string", props.get("skey"))
    assertEquals("600", props.get("ikey"))
    assertEquals("some-string-2", props.get("sokey"))
    assertEquals("700", props.get("iokey"))
    assertEquals("some-string-3", props.get("sondkey"))
    assertEquals("800", props.get("iondkey"))
  }

  @Test
  def testMaybeMergeOptionsDefaultOverwriteExisting(): Unit = {
    setUpOptions()

    props.put("sokey", "existing-string")
    props.put("iokey", "300")
    props.put("sondkey", "existing-string-2")
    props.put("iondkey", "400")

    val options = parser.parse(
      "--str-opt",
      "--int-opt",
      "--str-opt-nodef",
      "--int-opt-nodef",
    )

    ConfigUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    ConfigUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("default-string-2", props.get("sokey"))
    assertEquals("200", props.get("iokey"))
    assertNull(props.get("sondkey"))
    assertNull(props.get("iondkey"))
  }

  @Test
  def testMaybeMergeOptionsDefaultValueIfNotExist(): Unit = {
    setUpOptions()

    val options = parser.parse()

    ConfigUtils.maybeMergeOptions(props, "skey", options, stringOpt)
    ConfigUtils.maybeMergeOptions(props, "ikey", options, intOpt)
    ConfigUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    ConfigUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("default-string", props.get("skey"))
    assertEquals("100", props.get("ikey"))
    assertEquals("default-string-2", props.get("sokey"))
    assertEquals("200", props.get("iokey"))
    assertNull(props.get("sondkey"))
    assertNull(props.get("iondkey"))
  }

  @Test
  def testMaybeMergeOptionsNotOverwriteExisting(): Unit = {
    setUpOptions()

    props.put("skey", "existing-string")
    props.put("ikey", "300")
    props.put("sokey", "existing-string-2")
    props.put("iokey", "400")
    props.put("sondkey", "existing-string-3")
    props.put("iondkey", "500")

    val options = parser.parse()

    ConfigUtils.maybeMergeOptions(props, "skey", options, stringOpt)
    ConfigUtils.maybeMergeOptions(props, "ikey", options, intOpt)
    ConfigUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg)
    ConfigUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault)
    ConfigUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault)

    assertEquals("existing-string", props.get("skey"))
    assertEquals("300", props.get("ikey"))
    assertEquals("existing-string-2", props.get("sokey"))
    assertEquals("400", props.get("iokey"))
    assertEquals("existing-string-3", props.get("sondkey"))
    assertEquals("500", props.get("iondkey"))
  }
}
