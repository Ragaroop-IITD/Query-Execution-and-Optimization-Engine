package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * The join operator performs a Hash Join.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class JoinOperator extends OperatorBase implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private JoinPredicate predicate;

    // Hash join fields
    private Map<Object, List<Tuple>> hashTable;
    private Iterator<Tuple> rightTupleIterator;
    private Iterator<Tuple> matchIterator;
    private Tuple currentRightTuple;
    private List<String> joinedSchema;
    private boolean done;

    public JoinOperator(Operator leftChild, Operator rightChild, JoinPredicate predicate) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // Do not remove logging--
        logger.trace("Open()");
        // ----------------------

        // Open both child operators
        leftChild.open();
        rightChild.open();

        // Initialize the hash table
        hashTable = new HashMap<>();

        // Build the hash table from the left child (build phase)
        Tuple leftTuple;
        while ((leftTuple = leftChild.next()) != null) {
            // For each value in the tuple, we'll add it to the hash table
            // This approach handles the case where we don't know the exact join attribute
            for (Object value : leftTuple.getValues()) {
                if (value != null) {
                    hashTable.computeIfAbsent(value, k -> new ArrayList<>()).add(leftTuple);
                }
            }
        }

        // Initialize the right tuple iterator for the probe phase
        rightTupleIterator = new Iterator<Tuple>() {
            private Tuple nextTuple = null;
            private boolean hasCalledNext = false;

            @Override
            public boolean hasNext() {
                if (!hasCalledNext) {
                    nextTuple = rightChild.next();
                    hasCalledNext = true;
                }
                return nextTuple != null;
            }

            @Override
            public Tuple next() {
                if (!hasCalledNext) {
                    nextTuple = rightChild.next();
                }
                Tuple result = nextTuple;
                nextTuple = null;
                hasCalledNext = false;
                return result;
            }
        };

        matchIterator = null;
        currentRightTuple = null;
        done = false;
        joinedSchema = null;
    }

    @Override
    public Tuple next() {
        // Do not remove logging--
        logger.trace("Next()");
        // ----------------------

        if (done) {
            return null;
        }

        while (true) {
            // If we have an active iterator for matching left tuples, use it
            if (matchIterator != null && matchIterator.hasNext()) {
                Tuple leftTuple = matchIterator.next();
                return mergeTuples(leftTuple, currentRightTuple);
            }

            // Get next right tuple and find its matches
            if (!rightTupleIterator.hasNext()) {
                done = true;
                return null;
            }

            currentRightTuple = rightTupleIterator.next();

            // Collect potential matches from the hash table
            Set<Tuple> uniqueMatches = new HashSet<>();

            // Probe the hash table with each value from the right tuple
            for (Object value : currentRightTuple.getValues()) {
                if (value != null && hashTable.containsKey(value)) {
                    // For each potential match, verify with the predicate
                    for (Tuple leftTuple : hashTable.get(value)) {
                        if (predicate.evaluate(leftTuple, currentRightTuple)) {
                            uniqueMatches.add(leftTuple);
                        }
                    }
                }
            }

            if (!uniqueMatches.isEmpty()) {
                matchIterator = uniqueMatches.iterator();
            }
            // If no matches found, continue to the next right tuple
        }
    }

    @Override
    public void close() {
        // Do not remove logging ---
        logger.trace("Close()");
        // ------------------------

        if (leftChild != null) {
            leftChild.close();
        }

        if (rightChild != null) {
            rightChild.close();
        }

        // Clean up resources
        if (hashTable != null) {
            hashTable.clear();
            hashTable = null;
        }

        rightTupleIterator = null;
        matchIterator = null;
        currentRightTuple = null;
        joinedSchema = null;
        done = false;
    }

    // Helper method to merge two tuples
    private Tuple mergeTuples(Tuple left, Tuple right) {
        // Initialize schema if needed
        if (joinedSchema == null || joinedSchema.isEmpty()) {
            joinedSchema = new ArrayList<>();
            if (left != null && left.getSchema() != null) {
                joinedSchema.addAll(left.getSchema());
            }
            if (right != null && right.getSchema() != null) {
                joinedSchema.addAll(right.getSchema());
            }
        }

        // Merge values
        List<Object> mergedValues = new ArrayList<>();
        if (left != null && left.getValues() != null) {
            mergedValues.addAll(left.getValues());
        }
        if (right != null && right.getValues() != null) {
            mergedValues.addAll(right.getValues());
        }

        return new Tuple(mergedValues, joinedSchema);
    }

    // Do not remove these methods!
    public Operator getLeftChild() {
        return leftChild;
    }

    public Operator getRightChild() {
        return rightChild;
    }

    public JoinPredicate getPredicate() {
        return predicate;
    }
}