package in.ac.iitd.db362.optimizer;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.api.PlanPrinter;
import in.ac.iitd.db362.operators.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import in.ac.iitd.db362.storage.Tuple;
import java.util.*;

/**
 * A basic optimizer implementation. Feel free and be creative in designing your optimizer.
 * Do not change the constructor. Use the catalog for various statistics that are available.
 * For everything in your optimization logic, you are free to do what ever you want.
 * Make sure to write efficient code!
 */
public class BasicOptimizer implements Optimizer {

    // Do not remove or rename logger
    protected final Logger logger = LogManager.getLogger(this.getClass());

    // Do not remove or rename catalog. You'll need it in your optimizer
    private final Catalog catalog;

    /**
     * DO NOT CHANGE THE CONSTRUCTOR!
     *
     * @param catalog
     */
    public BasicOptimizer(Catalog catalog) {
        this.catalog = catalog;
    }

    /**
     * Basic optimization that currently does not modify the plan. Your goal is to come up with
     * an optimization strategy that should find an optimal plan. Come up with your own ideas or adopt the ones
     * discussed in the lecture to efficiently enumerate plans, a search strategy along with a cost model.
     *
     * @param plan The original query plan.
     * @return The (possibly) optimized query plan.
     */
    @Override
    public Operator optimize(Operator plan) {
        logger.info("Optimizing Plan:\n{}", PlanPrinter.getPlanString(plan));

        // First, we'll perform rule-based optimizations
        Operator optimizedPlan = applyRuleBasedOptimizations(plan);

        // Then, we'll apply cost-based optimizations
        optimizedPlan = applyCostBasedOptimizations(optimizedPlan);

        logger.info("Optimized Plan:\n{}", PlanPrinter.getPlanString(optimizedPlan));
        return optimizedPlan;
    }

    /**
     * Apply rule-based optimizations to the plan
     */
    private Operator applyRuleBasedOptimizations(Operator plan) {
        // Apply a series of rule-based optimizations
        Operator optimized = plan;

        // Push filters down as early as possible
        optimized = pushDownFilters(optimized);

        // Merge adjacent filters
        optimized = mergeFilters(optimized);

        // Optimize projections (push down, remove unnecessary ones)
        optimized = optimizeProjections(optimized);

        return optimized;
    }

    /**
     * Apply cost-based optimizations to the plan
     */
    private Operator applyCostBasedOptimizations(Operator plan) {
        // Apply cost-based optimizations
        Operator optimized = plan;

        // Optimize join orders based on relation sizes and selectivity
        optimized = optimizeJoins(optimized);

        return optimized;
    }

    /**
     * Push filters down closer to the data source
     */
    private Operator pushDownFilters(Operator op) {
        if (op == null) return null;

        // If this is a filter operator, try to push it down
        if (op instanceof FilterOperator) {
            FilterOperator filterOp = (FilterOperator) op;
            Operator child = filterOp.getChild();
            Predicate predicate = filterOp.getPredicate();

            // If child is a join, try to push the filter to one side of the join
            if (child instanceof JoinOperator) {
                JoinOperator joinOp = (JoinOperator) child;
                Operator left = joinOp.getLeftChild();
                Operator right = joinOp.getRightChild();
                JoinPredicate joinPred = joinOp.getPredicate();

                // Check if filter only references attributes from left child
                if (predicateOnlyReferencesAttributes(predicate, getOutputAttributes(left))) {
                    // Push filter to left side
                    Operator newLeft = new FilterOperator(left, predicate);
                    return new JoinOperator(newLeft, right, joinPred);
                }
                // Check if filter only references attributes from right child
                else if (predicateOnlyReferencesAttributes(predicate, getOutputAttributes(right))) {
                    // Push filter to right side
                    Operator newRight = new FilterOperator(right, predicate);
                    return new JoinOperator(left, newRight, joinPred);
                }
            }
            // If child is a project, try to push filter through the projection
            else if (child instanceof ProjectOperator) {
                ProjectOperator projectOp = (ProjectOperator) child;
                List<String> projectedColumns = projectOp.getProjectedColumns();

                // Check if filter only references projected columns
                if (predicateOnlyReferencesAttributes(predicate, new HashSet<>(projectedColumns))) {
                    // Push filter below projection if possible
                    Operator projectChild = projectOp.getChild();
                    Operator filteredChild = new FilterOperator(projectChild, predicate);
                    return new ProjectOperator(filteredChild, projectedColumns, projectOp.isDistinct());
                }
            }

            // Recursively optimize the child
            Operator optimizedChild = pushDownFilters(child);

            // If child was changed, create a new filter with the optimized child
            if (optimizedChild != child) {
                return new FilterOperator(optimizedChild, predicate);
            }
        }
        // For other operators, recursively optimize their children
        else if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            Operator leftChild = pushDownFilters(joinOp.getLeftChild());
            Operator rightChild = pushDownFilters(joinOp.getRightChild());

            // If any child was changed, create a new join with the optimized children
            if (leftChild != joinOp.getLeftChild() || rightChild != joinOp.getRightChild()) {
                return new JoinOperator(leftChild, rightChild, joinOp.getPredicate());
            }
        }
        else if (op instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) op;
            Operator child = pushDownFilters(projectOp.getChild());

            // If child was changed, create a new project with the optimized child
            if (child != projectOp.getChild()) {
                return new ProjectOperator(child, projectOp.getProjectedColumns(), projectOp.isDistinct());
            }
        }
        else if (op instanceof SinkOperator) {
            SinkOperator sinkOp = (SinkOperator) op;
            Operator child = pushDownFilters(sinkOp.getChild());

            // If child was changed, create a new sink with the optimized child
            if (child != sinkOp.getChild()) {
                return new SinkOperator(child, sinkOp.getOutputFile());
            }
        }

        // If no optimization was applied, return the original operator
        return op;
    }

    /**
     * Merge adjacent filter operations
     */
    private Operator mergeFilters(Operator op) {
        if (op == null) return null;

        // If this is a filter with a filter child, merge them
        if (op instanceof FilterOperator) {
            FilterOperator filterOp = (FilterOperator) op;
            Operator child = filterOp.getChild();

            if (child instanceof FilterOperator) {
                FilterOperator childFilterOp = (FilterOperator) child;

                // Create a composite predicate
                Predicate compositePredicate = new AndPredicate(
                        filterOp.getPredicate(), childFilterOp.getPredicate());

                // Recursively optimize the grandchild
                Operator optimizedGrandchild = mergeFilters(childFilterOp.getChild());

                // Create a new filter with the composite predicate
                return new FilterOperator(optimizedGrandchild, compositePredicate);
            }

            // Recursively optimize the child
            Operator optimizedChild = mergeFilters(child);

            // If child was changed, create a new filter with the optimized child
            if (optimizedChild != child) {
                return new FilterOperator(optimizedChild, filterOp.getPredicate());
            }
        }
        // For other operators, recursively optimize their children
        else if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            Operator leftChild = mergeFilters(joinOp.getLeftChild());
            Operator rightChild = mergeFilters(joinOp.getRightChild());

            // If any child was changed, create a new join with the optimized children
            if (leftChild != joinOp.getLeftChild() || rightChild != joinOp.getRightChild()) {
                return new JoinOperator(leftChild, rightChild, joinOp.getPredicate());
            }
        }
        else if (op instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) op;
            Operator child = mergeFilters(projectOp.getChild());

            // If child was changed, create a new project with the optimized child
            if (child != projectOp.getChild()) {
                return new ProjectOperator(child, projectOp.getProjectedColumns(), projectOp.isDistinct());
            }
        }
        else if (op instanceof SinkOperator) {
            SinkOperator sinkOp = (SinkOperator) op;
            Operator child = mergeFilters(sinkOp.getChild());

            // If child was changed, create a new sink with the optimized child
            if (child != sinkOp.getChild()) {
                return new SinkOperator(child, sinkOp.getOutputFile());
            }
        }

        // If no optimization was applied, return the original operator
        return op;
    }

    /**
     * Optimize projection operations
     */
    private Operator optimizeProjections(Operator op) {
        if (op == null) return null;

        // If this is a projection operator
        if (op instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) op;
            Operator child = projectOp.getChild();

            // If child is also a projection, potentially merge them
            if (child instanceof ProjectOperator) {
                ProjectOperator childProjectOp = (ProjectOperator) child;

                // If both are non-distinct or both are distinct, we can merge them
                if (projectOp.isDistinct() == childProjectOp.isDistinct()) {
                    // Create a mapping from child projection to its source
                    Map<String, String> columnMapping = createColumnMapping(childProjectOp);

                    // Apply the mapping to the parent projection columns
                    List<String> newProjections = applyColumnMapping(
                            projectOp.getProjectedColumns(), columnMapping);

                    // Create a new projection that goes directly to the child's child
                    return new ProjectOperator(
                            optimizeProjections(childProjectOp.getChild()),
                            newProjections,
                            projectOp.isDistinct()
                    );
                }
            }

            // Recursively optimize the child
            Operator optimizedChild = optimizeProjections(child);

            // If child was changed, create a new projection with the optimized child
            if (optimizedChild != child) {
                return new ProjectOperator(
                        optimizedChild,
                        projectOp.getProjectedColumns(),
                        projectOp.isDistinct()
                );
            }
        }
        // For other operators, recursively optimize their children
        else if (op instanceof FilterOperator) {
            FilterOperator filterOp = (FilterOperator) op;
            Operator child = optimizeProjections(filterOp.getChild());

            // If child was changed, create a new filter with the optimized child
            if (child != filterOp.getChild()) {
                return new FilterOperator(child, filterOp.getPredicate());
            }
        }
        else if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            Operator leftChild = optimizeProjections(joinOp.getLeftChild());
            Operator rightChild = optimizeProjections(joinOp.getRightChild());

            // If any child was changed, create a new join with the optimized children
            if (leftChild != joinOp.getLeftChild() || rightChild != joinOp.getRightChild()) {
                return new JoinOperator(leftChild, rightChild, joinOp.getPredicate());
            }
        }
        else if (op instanceof SinkOperator) {
            SinkOperator sinkOp = (SinkOperator) op;
            Operator child = optimizeProjections(sinkOp.getChild());

            // If child was changed, create a new sink with the optimized child
            if (child != sinkOp.getChild()) {
                return new SinkOperator(child, sinkOp.getOutputFile());
            }
        }

        // If no optimization was applied, return the original operator
        return op;
    }

    /**
     * Optimize join orders based on estimated costs
     */
    private Operator optimizeJoins(Operator op) {
        if (op == null) return null;

        // If this is a join operator
        if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;

            // Recursively optimize the children
            Operator leftChild = optimizeJoins(joinOp.getLeftChild());
            Operator rightChild = optimizeJoins(joinOp.getRightChild());

            // Estimate cardinality of each input
            long leftCardinality = estimateCardinality(leftChild);
            long rightCardinality = estimateCardinality(rightChild);

            // If the join can be reordered and it's beneficial (smaller relation on left)
            if (rightCardinality < leftCardinality && canSwapJoinOrder(joinOp)) {
                // Swap the join inputs
                return new JoinOperator(
                        rightChild,
                        leftChild,
                        swapJoinPredicate(joinOp.getPredicate())
                );
            }

            // If any child was changed but no reordering was done
            if (leftChild != joinOp.getLeftChild() || rightChild != joinOp.getRightChild()) {
                return new JoinOperator(leftChild, rightChild, joinOp.getPredicate());
            }
        }
        // For other operators, recursively optimize their children
        else if (op instanceof FilterOperator) {
            FilterOperator filterOp = (FilterOperator) op;
            Operator child = optimizeJoins(filterOp.getChild());

            // If child was changed, create a new filter with the optimized child
            if (child != filterOp.getChild()) {
                return new FilterOperator(child, filterOp.getPredicate());
            }
        }
        else if (op instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) op;
            Operator child = optimizeJoins(projectOp.getChild());

            // If child was changed, create a new project with the optimized child
            if (child != projectOp.getChild()) {
                return new ProjectOperator(
                        child,
                        projectOp.getProjectedColumns(),
                        projectOp.isDistinct()
                );
            }
        }
        else if (op instanceof SinkOperator) {
            SinkOperator sinkOp = (SinkOperator) op;
            Operator child = optimizeJoins(sinkOp.getChild());

            // If child was changed, create a new sink with the optimized child
            if (child != sinkOp.getChild()) {
                return new SinkOperator(child, sinkOp.getOutputFile());
            }
        }

        // If no optimization was applied, return the original operator
        return op;
    }

    // Helper methods

    /**
     * Check if a predicate only references attributes from a given set
     */
    private boolean predicateOnlyReferencesAttributes(Predicate predicate, Set<String> attributes) {
        // Get the attributes referenced by the predicate
        Set<String> referencedAttributes = getReferencedAttributes(predicate);

        // Check if all referenced attributes are in the given set
        return attributes.containsAll(referencedAttributes);
    }

    /**
     * Get attributes referenced by a predicate
     */
    private Set<String> getReferencedAttributes(Predicate predicate) {
        Set<String> attributes = new HashSet<>();

        // Implementation depends on the predicate structure
        if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate compPred = (ComparisonPredicate) predicate;

            // Add attributes from both operands
            if (compPred.getLeftOperand() instanceof String) {
                attributes.add((String) compPred.getLeftOperand());
            }
            if (compPred.getRightOperand() instanceof String) {
                attributes.add((String) compPred.getRightOperand());
            }
        }
        else if (predicate instanceof AndPredicate) {
            AndPredicate andPred = (AndPredicate) predicate;

            // Add attributes from both sub-predicates
            attributes.addAll(getReferencedAttributes(andPred.getLeftPredicate()));
            attributes.addAll(getReferencedAttributes(andPred.getRightPredicate()));
        }

        return attributes;
    }

    /**
     * Get output attributes of an operator
     */
    private Set<String> getOutputAttributes(Operator op) {
        Set<String> attributes = new HashSet<>();

        if (op instanceof ScanOperator) {
            ScanOperator scanOp = (ScanOperator) op;
            // Get schema from catalog
//            attributes.addAll(scanOp.getSchema());
            List<String> schema = scanOp.getSchema();
            if (schema != null) {
                attributes.addAll(schema);
            }

        }
        else if (op instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) op;
            attributes.addAll(projectOp.getProjectedColumns());
        }
        else if (op instanceof FilterOperator) {
            FilterOperator filterOp = (FilterOperator) op;
            attributes.addAll(getOutputAttributes(filterOp.getChild()));
        }
        else if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            attributes.addAll(getOutputAttributes(joinOp.getLeftChild()));
            attributes.addAll(getOutputAttributes(joinOp.getRightChild()));
        }

        return attributes;
    }

    /**
     * Create a mapping from output columns to source columns for a projection
     */
    private Map<String, String> createColumnMapping(ProjectOperator projectOp) {
        Map<String, String> mapping = new HashMap<>();
        List<String> projectedColumns = projectOp.getProjectedColumns();
        Set<String> inputAttributes = getOutputAttributes(projectOp.getChild());

        // For each projected column, find its source
        for (String column : projectedColumns) {
            if (inputAttributes.contains(column)) {
                mapping.put(column, column); // Direct mapping
            }
        }

        return mapping;
    }

    /**
     * Apply a column mapping to a list of columns
     */
    private List<String> applyColumnMapping(List<String> columns, Map<String, String> mapping) {
        List<String> result = new ArrayList<>();

        for (String column : columns) {
            String mappedColumn = mapping.get(column);
            if (mappedColumn != null) {
                result.add(mappedColumn);
            } else {
                result.add(column); // Keep original if no mapping exists
            }
        }

        return result;
    }

    /**
     * Estimate the cardinality (number of rows) of an operator
     */
    private long estimateCardinality(Operator op) {
        if (op instanceof ScanOperator) {
            ScanOperator scanOp = (ScanOperator) op;
            // Get cardinality from catalog
            return catalog.getTableStatistics(scanOp.getFilePath()).getNumRows();
        }
        else if (op instanceof FilterOperator) {
            FilterOperator filterOp = (FilterOperator) op;
            // Estimate cardinality based on selectivity
            double selectivity = estimateSelectivity(filterOp.getPredicate());
            return Math.round(estimateCardinality(filterOp.getChild()) * selectivity);
        }
        else if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            // Estimate join cardinality
            long leftCard = estimateCardinality(joinOp.getLeftChild());
            long rightCard = estimateCardinality(joinOp.getRightChild());
            double joinSelectivity = estimateJoinSelectivity(joinOp.getPredicate());
            return Math.round(leftCard * rightCard * joinSelectivity);
        }
        else if (op instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) op;
            // For distinct projections, estimate based on distinct values
            if (projectOp.isDistinct()) {
                // Simplistic estimate based on number of columns
                return Math.min(
                        estimateCardinality(projectOp.getChild()),
                        Math.round(Math.pow(10, projectOp.getProjectedColumns().size()))
                );
            } else {
                // Non-distinct projection doesn't change cardinality
                return estimateCardinality(projectOp.getChild());
            }
        }
        else if (op instanceof SinkOperator) {
            SinkOperator sinkOp = (SinkOperator) op;
            return estimateCardinality(sinkOp.getChild());
        }

        // Default estimate
        return 1000;
    }

    /**
     * Estimate selectivity of a predicate
     */
    private double estimateSelectivity(Predicate predicate) {
        // Default selectivity (can be improved with more statistics)
        if (predicate instanceof ComparisonPredicate) {
            return 0.3; // Typical selectivity for comparisons
        }
        else if (predicate instanceof AndPredicate) {
            AndPredicate andPred = (AndPredicate) predicate;
            // Combine selectivities for AND predicates
            return estimateSelectivity(andPred.getLeftPredicate()) *
                    estimateSelectivity(andPred.getRightPredicate());
        }
        return 0.5; // Default selectivity
    }

    /**
     * Estimate join selectivity
     */
    private double estimateJoinSelectivity(JoinPredicate predicate) {
        if (predicate instanceof EqualityJoinPredicate) {
            EqualityJoinPredicate eqJoin = (EqualityJoinPredicate) predicate;
            // Improve with catalog data on column uniqueness
            return 0.1; // Typical selectivity for equality joins
        }
        return 0.3; // Default selectivity
    }

    /**
     * Check if join inputs can be swapped
     */
    private boolean canSwapJoinOrder(JoinOperator joinOp) {
        // For equality joins, we can usually swap the order
        return joinOp.getPredicate() instanceof EqualityJoinPredicate;
    }

    /**
     * Swap the join predicate for when join inputs are swapped
     */
    private JoinPredicate swapJoinPredicate(JoinPredicate predicate) {
        if (predicate instanceof EqualityJoinPredicate) {
            EqualityJoinPredicate eqJoin = (EqualityJoinPredicate) predicate;
            // Swap left and right attributes
            return new EqualityJoinPredicate(eqJoin.getRightColumn(), eqJoin.getLeftColumn());
        }
        return predicate;
    }

    // Helper predicate classes

    private static class AndPredicate implements Predicate {
        private final Predicate leftPredicate, rightPredicate;

        public AndPredicate(Predicate leftPredicate, Predicate rightPredicate) {
            this.leftPredicate = leftPredicate;
            this.rightPredicate = rightPredicate;
        }

        public Predicate getLeftPredicate() {
            return leftPredicate;
        }

        public Predicate getRightPredicate() {
            return rightPredicate;
        }

        @Override
        public boolean evaluate(Tuple tuple) {
            return leftPredicate.evaluate(tuple) && rightPredicate.evaluate(tuple);
        }
    }
}