package kafka.utils.consoletable.enums;

import kafka.utils.consoletable.table.Cell;

public enum NullPolicy {

    THROW {
        @Override
        public Cell getCell(Cell cell) {
            throw new IllegalArgumentException("cell or value is null: " + cell);
        }
    },
    NULL_STRING {
        @Override
        public Cell getCell(Cell cell) {
            if(cell == null){
                return new Cell("null");
            }
            cell.setValue("null");
            return cell;
        }
    },
    EMPTY_STRING {
        @Override
        public Cell getCell(Cell cell) {
            if(cell == null){
                return new Cell("");
            }
            cell.setValue("");
            return cell;
        }
    };

    public abstract Cell getCell(Cell cell);

}
