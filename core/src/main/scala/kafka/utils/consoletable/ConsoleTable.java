package kafka.utils.consoletable;

import kafka.utils.consoletable.enums.NullPolicy;
import kafka.utils.consoletable.table.Body;
import kafka.utils.consoletable.table.Cell;
import kafka.utils.consoletable.table.Header;
import kafka.utils.consoletable.util.StringPadUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ConsoleTable {

    private Header header;
    private Body body;
    String lineSep = "\n";
    String verticalSep = "|";
    String horizontalSep = "-";
    String joinSep = "+";
    int[] columnWidths;
    int[] lastColumnWidths = new int[0];
    NullPolicy nullPolicy = NullPolicy.EMPTY_STRING;
    boolean restrict = false;

    private ConsoleTable(){}

    public void printContent() {
        System.out.println(getContent());
    }

    String getContent() {
        return toString();
    }

    List<String> getLines(){
        List<String> lines = new ArrayList<>();
        boolean isBottom = true;
        if (header != null && !header.isEmpty()) {
            lines.addAll(header.print(columnWidths, horizontalSep, verticalSep, joinSep));
            isBottom = false;
        }
        if (body != null && !body.isEmpty()) {
            lines.addAll(body.print(columnWidths,horizontalSep,verticalSep,joinSep));
            lastColumnWidths = columnWidths;
            isBottom = false;
        }

        return lines;
    }

    @Override
    public String toString() {
        return StringUtils.join(getLines(), lineSep);
    }

    public static class ConsoleTableBuilder {

        ConsoleTable consoleTable = new ConsoleTable();

        public ConsoleTableBuilder(){
            consoleTable.header = new Header();
            consoleTable.body = new Body();
        }

        public ConsoleTableBuilder addHead(Cell cell){
            consoleTable.header.addHead(cell);
            return this;
        }

        public ConsoleTableBuilder addRow(List<Cell> row){
            consoleTable.body.addRow(row);
            return this;
        }

        public ConsoleTableBuilder addHeaders(List<Cell> headers){
            consoleTable.header.addHeads(headers);
            return this;
        }

        public ConsoleTableBuilder addRows(List<List<Cell>> rows){
            consoleTable.body.addRows(rows);
            return this;
        }

        public ConsoleTableBuilder clearHeaders() {
            consoleTable.header.clearHeads();
            return this;
        }

        public ConsoleTableBuilder clearRows() {
            consoleTable.body.clearRows();
            return this;
        }

        public ConsoleTableBuilder clearAll() {
            clearHeaders();
            clearRows();
            return this;
        }

        public ConsoleTableBuilder lineSep(String lineSep){
            consoleTable.lineSep = lineSep;
            return this;
        }

        public ConsoleTableBuilder verticalSep(String verticalSep){
            consoleTable.verticalSep = verticalSep;
            return this;
        }

        public ConsoleTableBuilder horizontalSep(String horizontalSep){
            consoleTable.horizontalSep = horizontalSep;
            return this;
        }

        public ConsoleTableBuilder joinSep(String joinSep){
            consoleTable.joinSep = joinSep;
            return this;
        }

        public ConsoleTableBuilder nullPolicy(NullPolicy nullPolicy){
            consoleTable.nullPolicy = nullPolicy;
            return this;
        }

        public ConsoleTableBuilder restrict(boolean restrict){
            consoleTable.restrict = restrict;
            return this;
        }

        public ConsoleTable build(){
            //compute max column widths
            if(!consoleTable.header.isEmpty() || !consoleTable.body.isEmpty()){
                List<List<Cell>> allRows = new ArrayList<>();
                allRows.add(consoleTable.header.getCells());
                allRows.addAll(consoleTable.body.getRows());
                int maxColumn = allRows.stream().map(List::size).mapToInt(size -> size).max().getAsInt();
                int minColumn = allRows.stream().map(List::size).mapToInt(size -> size).min().getAsInt();
                if(maxColumn != minColumn && consoleTable.restrict){
                    throw new IllegalArgumentException("number of columns for each row must be the same when strict mode used.");
                }
                consoleTable.columnWidths = new int[maxColumn];
                for (List<Cell> row : allRows) {
                    for (int i = 0; i < row.size(); i++) {
                        Cell cell = row.get(i);
                        if(cell == null || cell.getValue() == null){
                            cell = consoleTable.nullPolicy.getCell(cell);
                            row.set(i,cell);
                        }
                        int length = StringPadUtil.strLength(cell.getValue());
                        if(consoleTable.columnWidths[i] < length){
                            consoleTable.columnWidths[i] = length;
                        }
                    }
                }
            }
            return consoleTable;
        }
    }
}
