package in.ac.iitd.db362.catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * TableStatistics stores column statistics for each column of a table. In
 * addition it stores the number of rows in a table.
 */
public class TableStatistics {

    // Maps column name to its statistics
    private final Map<String, ColumnStatistics<?>> columnStats;

    private final int numRows;
    public TableStatistics(int numRows) {
        this.numRows = numRows;
        this.columnStats = new HashMap<>();
    }

    public void addColumnStatistics(String columnName, ColumnStatistics<?> stats) {
        columnStats.put(columnName, stats);
    }

    /**
     * ColumnStatistics for a column
     * @param columnName
     * @return
     */
    public ColumnStatistics<?> getColumnStatistics(String columnName) {
        return columnStats.get(columnName);
    }

    /**
     * The number of rows (csv lines excluding the header line)
     * @return
     */
    public int getNumRows() {
        return numRows;
    }
}
