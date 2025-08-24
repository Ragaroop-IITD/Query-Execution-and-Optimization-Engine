package in.ac.iitd.db362.api;

import in.ac.iitd.db362.operators.*;

import java.util.*;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * PlanBuilder provides a convenient fluent API to directly build execution plans.
 */
public class PlanBuilder {
    private Operator root;

    private PlanBuilder(Operator root) {
        this.root = root;
    }

    public static PlanBuilder scan(String csvFile) {
        return new PlanBuilder(new ScanOperator(csvFile));
    }

    public PlanBuilder filter(Predicate predicate) {
        root = new FilterOperator(root, predicate);
        return this;
    }

    /**
     * Overloaded filter that accepts a predicate string.
     * The string should be in the form: "attribute op constant"
     * where op is one of =, >, >=, <, <=.
     */
    public PlanBuilder filter(String predicateString) {
        Predicate predicate = parsePredicate(predicateString);
        return filter(predicate);
    }

    public PlanBuilder project(String... columns) {
        return project(false, columns);
    }

    public PlanBuilder projectDistinct(String... columns) {
        return project(true, columns);
    }

    public PlanBuilder project(boolean distinct, String... columns) {
        root = new ProjectOperator(root, Arrays.asList(columns), distinct);
        return this;
    }

    public PlanBuilder join(PlanBuilder other, JoinPredicate joinPredicate) {
        root = new JoinOperator(root, other.build(), joinPredicate);
        return this;
    }

    /**
     * Overloaded join that accepts a join predicate string.
     * The string should be in the form: "leftAttr = rightAttr"
     * (currently only equality joins are supported).
     */
    public PlanBuilder join(PlanBuilder other, String joinPredicateString) {
        JoinPredicate joinPredicate = parseJoinPredicate(joinPredicateString);
        return join(other, joinPredicate);
    }

    public PlanBuilder sink(String outputFile) {
        root = new SinkOperator(root, outputFile);
        return this;
    }

    public Operator build() {
        return root;
    }

    // -------------------------------
    // Helper Methods for Parsing
    // -------------------------------

    /**
     * Parses an atomic filter predicate from a string.
     * Expected format: "attribute op constant"
     * where op is one of =, >, >=, <, <=, !=.
     */
    private static Predicate parsePredicate(String predicateStr) {
        predicateStr = predicateStr.trim();
        String operator = null;

        // Check longer operators first.
        if (predicateStr.contains(">=")) {
            operator = ">=";
        } else if (predicateStr.contains("<=")) {
            operator = "<=";
        } else if (predicateStr.contains("!=")) {
            operator = "!=";
        } else if (predicateStr.contains(">")) {
            operator = ">";
        } else if (predicateStr.contains("<")) {
            operator = "<";
        } else if (predicateStr.contains("=")) {
            operator = "=";
        } else {
            throw new IllegalArgumentException("Unsupported operator in predicate: " + predicateStr);
        }

        String[] parts = predicateStr.split("\\Q" + operator + "\\E");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid predicate format: " + predicateStr);
        }

        Object leftOperand = parseOperand(parts[0].trim());
        Object rightOperand = parseOperand(parts[1].trim());

        return new ComparisonPredicate(leftOperand, operator, rightOperand);
    }

    /**
     * Parses a join predicate string.
     * Expected format: "leftAttr = rightAttr"
     */
    private static JoinPredicate parseJoinPredicate(String joinPredicateStr) {
        joinPredicateStr = joinPredicateStr.trim();
        if (!joinPredicateStr.contains("=")) {
            throw new IllegalArgumentException("Only equality join predicates are supported: " + joinPredicateStr);
        }

        String[] parts = joinPredicateStr.split("=");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid join predicate format: " + joinPredicateStr);
        }

        // For join predicates, both operands are expected to be attribute names.
        String leftAttr = parts[0].trim();
        String rightAttr = parts[1].trim();

        return new EqualityJoinPredicate(leftAttr, rightAttr);
    }

    /**
     * Helper method to parse an operand.
     * If the operand is enclosed in quotes, it is treated as a string constant.
     * Otherwise, it tries to parse as an integer, then double, and if both fail,
     * it is treated as an attribute reference (i.e. a column name).
     */
    private static Object parseOperand(String operand) {
        // If operand is enclosed in quotes, treat it as a string constant.
        if ((operand.startsWith("\"") && operand.endsWith("\"")) ||
                (operand.startsWith("'") && operand.endsWith("'"))) {
            return operand.substring(1, operand.length() - 1);
        }
        // Try to parse as integer.
        try {
            return Integer.parseInt(operand);
        } catch (NumberFormatException e) {
            // Not an integer.
        }
        // Try to parse as double.
        try {
            return Double.parseDouble(operand);
        } catch (NumberFormatException e) {
            // Not a double.
        }
        // Otherwise, treat as attribute (column) reference.
        return operand;
    }
}
