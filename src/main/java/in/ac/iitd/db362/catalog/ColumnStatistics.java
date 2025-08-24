package in.ac.iitd.db362.catalog;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * Each column based on its type (integer, double, or string) implements this
 * interface
 *
 * @param <T>
 */
public interface ColumnStatistics<T> {
    int getCardinality(); // Number of unique values
    T getMin();
    T getMax();
    int[] getHistogram(); // Equi-width histogram bucket counts
    int getNumValues();
}
