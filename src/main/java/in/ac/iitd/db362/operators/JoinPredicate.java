package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

/**
 * DO NOT CHANGE THIS FILE
 *
 *
 */
public interface JoinPredicate {
    /**
     * Evaluates the join condition on two tuples.
     *
     * @param left  the tuple from the left input
     * @param right the tuple from the right input
     * @return true if the join condition is satisfied, false otherwise
     */
    boolean evaluate(Tuple left, Tuple right);
}
