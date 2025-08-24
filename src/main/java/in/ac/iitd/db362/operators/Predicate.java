package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 */
public interface Predicate {
    /**
     * Evaluates the predicate on the given tuple.
     *
     * @param tuple the tuple to evaluate
     * @return true if the tuple satisfies the predicate, false otherwise
     */    boolean evaluate(Tuple tuple);
}
