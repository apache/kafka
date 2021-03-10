package kafka.utils.consoletable.table;

import kafka.utils.consoletable.util.PrintUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Header {

    private List<Cell> cells;

    public Header(){
        this.cells = new ArrayList<>();
    }

    public void addHead(Cell cell){
        cells.add(cell);
    }

    public void addHeads(List<Cell> headers){
        cells.addAll(headers);
    }

    public List<Cell> getCells() {
        return cells;
    }

    public void clearHeads() {
        cells.clear();
    }

    public boolean isEmpty(){
        return cells == null || cells.isEmpty();
    }

    /**
     * print header including top and bottom sep
     * @param columnWidths max width of each column
     * @param horizontalSep char of h-sep, default '-'
     * @param verticalSep char of v-sep, default '|'
     * @param joinSep char of corner, default '+'
     * @return like:
     * +------------+--------------+------------+
     * | one        | two          | three      |
     * +------------+--------------+------------+
     * bottom will be printed by the body, for more completely output when there is no header sometimes
     */
    public List<String> print(int[] columnWidths, String horizontalSep, String verticalSep, String joinSep){
        List<String> result = new ArrayList<>();
        if(!isEmpty()){
            //top horizontal sep line
            result.addAll(PrintUtil.printLineSep(columnWidths,horizontalSep, verticalSep, joinSep));
            //header row
            result.addAll(PrintUtil.printRows(Collections.singletonList(cells),columnWidths,verticalSep));
        }
        return result;
    }
}
