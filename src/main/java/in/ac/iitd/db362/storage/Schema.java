package in.ac.iitd.db362.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * A schema is simply a list of ColumnMeta objects.
 */
public class Schema {
    private final List<ColumnMeta> columns;

    public Schema() {
        this.columns = new ArrayList<>();
    }

    public void addColumnMeta(ColumnMeta meta) {
        columns.add(meta);
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }
}
