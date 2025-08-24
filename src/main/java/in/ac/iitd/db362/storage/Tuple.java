package in.ac.iitd.db362.storage;

import java.util.List;

/**
 * DO NOT MODIFY THIS CLASS
 *
 * A Naive implementation of a tuple.
 * A tuple is list of values and along with column names
 */
public class Tuple {
    private List<Object> values;
    private List<String> schema; // Column names

    public Tuple(List<Object> values, List<String> schema) {
        this.values = values;
        this.schema = schema;
    }

    public Object get(int index) {
        return values.get(index);
    }

    public Object get(String columnName) {
        int idx = schema.indexOf(columnName);
        if (idx < 0) {
            throw new IllegalArgumentException("Column " + columnName + " not found");
        }
        return values.get(idx);
    }

    public List<Object> getValues() {
        return values;
    }

    public List<String> getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
