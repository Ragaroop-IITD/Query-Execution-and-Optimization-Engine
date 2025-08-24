package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

/**
 * The filter operator produces tuples that satisfy the predicate
 * TODO: Implement the open(), next(), and close() methods.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class FilterOperator extends OperatorBase implements Operator {
    private Operator child;
    private Predicate predicate;

    public FilterOperator(Operator child, Predicate predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // ------------------------

        // Open the child operator to start the pipeline
        child.open();
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // -------------------------

        // Keep getting tuples from child until we find one that satisfies the predicate
        // or we reach the end of the child's tuples
        while (true) {
            // Get the next tuple from child
            Tuple tuple = child.next();

            // If no more tuples, return null
            if (tuple == null) {
                return null;
            }

            // Apply the predicate to the tuple
            if (predicate.evaluate(tuple)) {
                // If predicate is satisfied, return this tuple
                return tuple;
            }

            // If predicate is not satisfied, continue to the next tuple
        }

        // remove and return the next tuple
//        throw new RuntimeException("Method not yet implemented");
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // -------------------------

        // Close the child operator
        child.close();
    }


    // Do not remove these methods!
    public Operator getChild() {
        return child;
    }

    public Predicate getPredicate() {
        return predicate;
    }
}
