package in.ac.iitd.db362.optimizer;

import in.ac.iitd.db362.operators.Operator;

/**
 * DO NOT MODIFY THIS CLASS
 */
public interface Optimizer {
    /**
     * Optimizes the given query plan (operator tree) by reordering or rewriting operators
     * to achieve better performance.
     *
     * @param plan The root of the operator tree representing the query plan.
     * @return The optimized query plan.
     */
    Operator optimize(Operator plan);
}
