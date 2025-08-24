package in.ac.iitd.db362.storage;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * This class represents metadata for a column, including its name and type.
 */
public class ColumnMeta {
    private final String name;
    private final String type; // Expected values: "int", "double", "string"

    public ColumnMeta(String name, String type) {
        this.name = name;
        this.type = type.toLowerCase();
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
