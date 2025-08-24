package in.ac.iitd.db362.storage;

/**
 * DO NOT MODIFY THIS CLASS
 *
 * The Table class ties together the schema and the table name.
 * Note the table does not hold the actual csv file!
 */
public class Table {
    private final String tableName;
    private final Schema schema;

    public Table(String tableName, Schema schema) {
        this.tableName = tableName;
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }
}
