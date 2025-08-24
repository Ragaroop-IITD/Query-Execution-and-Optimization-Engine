package in.ac.iitd.db362.catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * The catalog stores some basic statistics about data.
 */
public class Catalog {

    // Maps table name to table statistics
    private final Map<String, TableStatistics> tableStats;

    // Maps column name to table name
    private final Map<String, String> colToTableMap;

    public Catalog() {
        this.tableStats = new HashMap<>();
        this.colToTableMap = new HashMap<>();
    }

    public void registerTable(String tableName, TableStatistics stats) {
        tableStats.put(tableName, stats);
    }


    public void registerColumn(String colName, String tableName) {
        colToTableMap.put(colName, tableName);
    }


    /**
     * Function to return TableStatistics for a given table
     * @param tableName
     * @return
     */
    public TableStatistics getTableStatistics(String tableName) {
        return tableStats.get(tableName);
    }


    /**
     * Function that returns table name (string) for a given column name
     * @param columnName
     * @return table name
     */
    public String getTableForColumn(String columnName) {
        return colToTableMap.get(columnName);
    }
}
