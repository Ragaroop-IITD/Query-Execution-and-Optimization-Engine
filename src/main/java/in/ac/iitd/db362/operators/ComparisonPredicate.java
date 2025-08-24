package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Note: ONLY IMPLEMENT THE EVALUATE METHOD.
 * TODO: Implement the evaluate() method
 *
 * DO NOT CHANGE the constructor or existing member variables.
 *
 * A comparison predicate for simple atomic predicates.
 */
public class ComparisonPredicate implements Predicate {

    protected final static Logger logger = LogManager.getLogger();
    private final Object leftOperand;   // Either a constant or a column reference (String)
    private final String operator;        // One of: =, >, >=, <, <=, !=
    private final Object rightOperand;  // Either a constant or a column reference (String)

    public ComparisonPredicate(Object leftOperand, String operator, Object rightOperand) {
        this.leftOperand = leftOperand;
        this.operator = operator;
        this.rightOperand = rightOperand;
    }

    /**
     * Evaluate a tuple
     * @param tuple the tuple to evaluate
     * @return return true if leftOperator operator righOperand holds in that tuple
     */
    @Override
    public boolean evaluate(Tuple tuple) {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Evaluating tuple " + tuple.getValues() + " with schema " + tuple.getSchema());
        logger.trace("[Predicate] " + leftOperand + " " + operator + " " + rightOperand);
        // -------------------------

        // Resolve the left operand value
        Object leftValue;
        if (leftOperand instanceof String && tuple.getSchema().contains(leftOperand)) {
            leftValue = tuple.get((String) leftOperand);
        } else {
            leftValue = leftOperand;
        }

        // Resolve the right operand value
        Object rightValue;
        if (rightOperand instanceof String && tuple.getSchema().contains(rightOperand)) {
            rightValue = tuple.get((String) rightOperand);
        } else {
            rightValue = rightOperand;
        }

        // Handle null values
        if (leftValue == null || rightValue == null) {
            return false; // SQL semantics: NULL comparison always evaluates to false
        }

        // Ensure the operands are comparable
        if (!(leftValue instanceof Comparable) || !(rightValue instanceof Comparable)) {
            throw new RuntimeException("Non-comparable values: " + leftValue + " and " + rightValue);
        }

        // Make sure the types are compatible for comparison
        if (!leftValue.getClass().equals(rightValue.getClass())) {
            // If comparing number types, convert to double for comparison
            if (leftValue instanceof Number && rightValue instanceof Number) {
                leftValue = ((Number) leftValue).doubleValue();
                rightValue = ((Number) rightValue).doubleValue();
            } else {
                // Otherwise, convert to string for simple comparison
                leftValue = leftValue.toString();
                rightValue = rightValue.toString();
            }
        }

        // Perform the comparison
        @SuppressWarnings("unchecked")
        Comparable<Object> leftComparable = (Comparable<Object>) leftValue;
        int compareResult = leftComparable.compareTo(rightValue);

        switch (operator) {
            case "=":
                return compareResult == 0;
            case ">":
                return compareResult > 0;
            case ">=":
                return compareResult >= 0;
            case "<":
                return compareResult < 0;
            case "<=":
                return compareResult <= 0;
            case "!=":
                return compareResult != 0;
            default:
                throw new RuntimeException("Unknown operator: " + operator);
        }
    }

    // DO NOT REMOVE these functions! ---
    @Override
    public String toString() {
        return "ComparisonPredicate[" +
                "leftOperand=" + leftOperand +
                ", operator='" + operator + '\'' +
                ", rightOperand=" + rightOperand +
                ']';
    }
    public Object getLeftOperand() {
        return leftOperand;
    }

    public String getOperator() {
        return operator;
    }
    public Object getRightOperand() {
        return rightOperand;
    }

}
