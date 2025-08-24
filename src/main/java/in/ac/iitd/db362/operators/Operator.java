package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

/**
 * DO NOT CHANGE THIS FILE
 *
 * The operator interface. Each query processing operator must implement this interface
 */
public interface Operator {
    /**
     * Prepares the operator for execution.
     */
    void open();

    /**
     * Returns the next tuple in the pipeline, or null if none remain.
     */
    Tuple next();

    /**
     * Releases any resources held by the operator.
     */
    void close();
}
