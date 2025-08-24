package in.ac.iitd.db362.catalog;

/**
 *
 * MODIFY/UPDATE -- DO WHAT EVER YOU WANT WITH THIS CLASS!
 *
 * This class provides some methods that the optimizer can use to query the catalog.
 * The starter code is only for your reference. **Feel free** to modify this by
 * implementing the methods * and/or by adding your own methods that you think your
 * optimizer can use when optimizing plans.
 */
public class StatisticsQueryService {
    private final Catalog catalog;

    public StatisticsQueryService(Catalog catalog) {
        this.catalog = catalog;
    }

    public double getEqualitySelectivity(String tableName, String columnName, Object value) {
        throw new UnsupportedOperationException("Please implement me first!");
    }

    public double getEqualitySelectivityUsingHistogram(String tableName, String columnName, Object value) {
        throw new UnsupportedOperationException("Please implement me first!");
    }


    public double getRangeSelectivity(String tableName, String columnName, Object lowerBound, Object upperBound) {
        throw new UnsupportedOperationException("Please implement me first!");
    }


    public double getRangeSelectivityUsingHistogram(String tableName, String columnName, Object lowerBound, Object upperBound) {
        throw new UnsupportedOperationException("Please implement me first!");
    }


    public Object getMin(String tableName, String columnName) {
        throw new UnsupportedOperationException("Please implement me first!");
    }


    public Object getMax(String tableName, String columnName) {
        return null;
    }
}
