package unit.kafka.utils.consoletable

import java.util

import kafka.utils.consoletable.ConsoleTable
import kafka.utils.consoletable.table.Cell
import org.junit.jupiter.api.Test

class ConsoleTableTest {
  private val builder = new ConsoleTable.ConsoleTableBuilder
  @Test
  def testConsoleTablePrint(): Unit = {
    val header = new util.ArrayList[Cell]() {
      add(new Cell("item"))
      add(new Cell("category"))
      add(new Cell("alias"))
    }
    val body = new util.ArrayList[util.List[Cell]]() {
      add(new util.ArrayList[Cell]() {
        add(new Cell("console"))
        add(new Cell("table"))
        add(new Cell("Kafka"))
      })
    }

    builder.addHeaders(header).addRows(body).build().printContent()
  }
}
