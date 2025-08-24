package in.ac.iitd.db362.catalog;

import it.unimi.dsi.fastutil.doubles.*;

/**
 * DO NOT MODIFY THIS CLASS
 *
 * Implementation of column statistics for double data type
 */
public class DoubleColumnStatistics implements ColumnStatistics<Double> {
    private final DoubleOpenHashSet uniqueValues;
    private double min;
    private double max;
    private final int[] histogram;
    private final int NUM_BUCKETS = 10;

    private final int numValues;

    public DoubleColumnStatistics(DoubleList columnData) {
        numValues = columnData.size();
        uniqueValues = new DoubleOpenHashSet();
        histogram = new int[NUM_BUCKETS];
        if (columnData.isEmpty()) return;

        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;

        // Compute min/max
        for (double value : columnData) {
            uniqueValues.add(value);
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        // Compute histogram
        double bucketSize = (max - min) / NUM_BUCKETS;
        for (double value : columnData) {
            int bucket = Math.min((int) ((value - min) / bucketSize), NUM_BUCKETS - 1);
            histogram[bucket]++;
        }
    }

    public double[] getUniqueValues() {
        return uniqueValues.toDoubleArray();
    }

    /**
     * Cardinality refers to number of distinct values.
     * @return
     */
    public int getCardinality() {
        return uniqueValues.size();
    }

    public Double getMin() {
        return min;
    }

    public Double getMax() {
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
}
