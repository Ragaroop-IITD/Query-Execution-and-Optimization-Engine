package in.ac.iitd.db362.catalog;

import it.unimi.dsi.fastutil.ints.*;

/**
 * DO NOT MODIFY THIS CLASS
 *
 * Implementation of column statistics for integer data type
 */
public class IntColumnStatistics implements ColumnStatistics<Integer> {
    private final IntOpenHashSet uniqueValues; // Cardinality
    private int min;
    private int max;
    private final int[] histogram; // Equi-width histogram
    private final int NUM_BUCKETS = 10;

    private final int numValues;

    public IntColumnStatistics(IntList columnData) {
        numValues = columnData.size();
        uniqueValues = new IntOpenHashSet();
        histogram = new int[NUM_BUCKETS];
        if (columnData.isEmpty()) return;

        min = Integer.MAX_VALUE;
        max = Integer.MIN_VALUE;

        // Compute min/max
        for (int value : columnData) {
            uniqueValues.add(value);
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        // Compute histogram
        int bucketSize = (max - min + 1) / NUM_BUCKETS;
        for (int value : columnData) {
            int bucket = Math.min((value - min) / bucketSize, NUM_BUCKETS - 1);
            histogram[bucket]++;
        }
    }

    /**
     * Cardinality refers to number of distinct values.
     * @return
     */
    public int getCardinality() {
        return uniqueValues.size();
    }

    public Integer getMin() {
        return min;
    }

    public Integer getMax() {
        return max;
    }

    public int[] getHistogram() {
        return histogram;
    }

    /**
     * Function to get number of values for this column including duplicates
     * @return
     */
    public int getNumValues() {
        return numValues;
    }

    public int[] getUniqueValues() {
        return uniqueValues.toIntArray();
    }
}
