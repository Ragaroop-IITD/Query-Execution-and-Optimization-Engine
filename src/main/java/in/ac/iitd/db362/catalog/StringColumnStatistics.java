package in.ac.iitd.db362.catalog;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.List;

/**
 * DO NOT MODIFY THIS CLASS
 *
 * Implementation of column statistics for string data type
 */
public class StringColumnStatistics implements ColumnStatistics<String> {


    private int numValues = 0;

    private int cardinality = 0;

    public StringColumnStatistics(List<String> columnData) {
        if (columnData.isEmpty()) return;

        numValues = columnData.size();
        ObjectOpenHashSet<String> uniqueValues = new ObjectOpenHashSet<>();

        for (String value : columnData) {
            uniqueValues.add(value);
        }
        cardinality = uniqueValues.size();

        // We don't need this anymore so lets get rid of this
        uniqueValues.clear();
    }


    /**
     * Cardinality refers to number of distinct values.
     * @return
     */
    public int getCardinality() {
        return cardinality;
    }

    public String getMin() {
        throw new UnsupportedOperationException("No MIN value supported for String data type");
    }

    public String getMax() {
        throw new UnsupportedOperationException("No MAX value supported for String data type");
    }

    public int[] getHistogram() {
        throw new UnsupportedOperationException("No Histogram available for String columns");
    }

    /**
     * Function to get number of values for this column including duplicates
     * @return
     */
    @Override
    public int getNumValues() {
        return numValues;
    }
}
