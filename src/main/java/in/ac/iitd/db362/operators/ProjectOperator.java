package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * Implementation of a simple project operator that implements the operator interface.
 *
 *
 * TODO: Implement the open(), next(), and close() methods!
 * Do not change the constructor or existing member variables.
 */
public class ProjectOperator extends OperatorBase implements Operator {
    private Operator child;
    private List<String> projectedColumns;
    private boolean distinct;
    private Set<List<Object>> seenTuples;


    /**
     * Project operator. If distinct is set to true, it does duplicate elimination
     * @param child
     * @param projectedColumns
     * @param distinct
     */
    public ProjectOperator(Operator child, List<String> projectedColumns, boolean distinct) {
        this.child = child;
        this.projectedColumns = projectedColumns;
        this.distinct = distinct;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // -------------------------

        // Initialize the child operator
        child.open();

        // Initialize distinct set if needed
        if (distinct) {
            seenTuples = new HashSet<>();
        }
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // ------------------------

        // Keep fetching tuples from child until we find a non-duplicate (if distinct is true)
        while (true) {
            // Get next tuple from child
            Tuple childTuple = child.next();

            // If no more tuples, return null
            if (childTuple == null) {
                return null;
            }

            // Project only the requested columns
            List<Object> projectedValues = new ArrayList<>();
            for (String columnName : projectedColumns) {
                projectedValues.add(childTuple.get(columnName));
            }

            // Check for duplicates if distinct is true
            if (distinct) {
                if (seenTuples.contains(projectedValues)) {
                    // Skip this duplicate tuple and continue to the next one
                    continue;
                }
                // Add to seen tuples set
                seenTuples.add(projectedValues);
            }

            // Create and return the projected tuple
            return new Tuple(projectedValues, projectedColumns);
        }

        // remove me after implementation
//        throw new RuntimeException("Method not yet implemented");
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // ------------------------

        // Close the child operator
        child.close();

        // Clear distinct set if it exists
        if (distinct && seenTuples != null) {
            seenTuples.clear();
            seenTuples = null;
        }
    }

    // do not remvoe these methods!
    public Operator getChild() {
        return child;
    }

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public boolean isDistinct() {
        return distinct;
    }
}
