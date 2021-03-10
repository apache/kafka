package kafka.utils.consoletable.table;

import kafka.utils.consoletable.util.PrintUtil;

import java.util.ArrayList;
import java.util.List;

public class Body {

    private List<List<Cell>> rows;

    public Body() {
        rows = new ArrayList<>();
    }

    public void addRow(List<Cell> row) {
        this.rows.add(row);
    }

    public void addRows(List<List<Cell>> rows) {
        this.rows.addAll(rows);
    }

    public List<List<Cell>> getRows() {
        return rows;
    }

    public void clearRows() {
        rows.clear();
    }

    public boolean isEmpty() {
        return rows == null || rows.isEmpty();
    }

    /**
     * print header including top and bottom sep
     *
     * @param columnWidths  max width of each column
     * @param horizontalSep char of h-sep, default '-'
     * @param verticalSep   char of v-sep, default '|'
     * @param joinSep       char of corner, default '+'
     * @return like:
     * | one        | two          | three      |
     * +------------+--------------+------------+
     */
    public List<String> print(int[] columnWidths, String horizontalSep, String verticalSep, String joinSep) {
        List<String> result = new ArrayList<>();
        if (!isEmpty()) {
            //top horizontal sep line
            result.addAll(PrintUtil.printLineSep(columnWidths,horizontalSep, verticalSep, joinSep));
            //rows
            result.addAll(PrintUtil.printRows(rows,columnWidths,verticalSep));
            //bottom horizontal sep line
            result.addAll(PrintUtil.printLineSep(columnWidths,horizontalSep, verticalSep, joinSep));
        }
        return result;
    }
}
