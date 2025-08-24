package in.ac.iitd.db362.api;

import in.ac.iitd.db362.operators.*;


/**
 * DO NOT MODIFY THIS CLASS!
 *
 * A simple utility to print operator trees
 */
public class PlanPrinter {
    /**
     * Returns the execution plan as a formatted string.
     *
     * @param root The root operator of the execution plan.
     * @return Formatted execution plan string.
     */
    public static String getPlanString(Operator root) {
        StringBuilder sb = new StringBuilder();
        buildPlanString(root, 0, sb);
        return sb.toString();
    }

    private static void buildPlanString(Operator op, int indentLevel, StringBuilder sb) {
        String indent = "  ".repeat(indentLevel); // Indentation for tree-like structure

        if (op instanceof ScanOperator) {
            ScanOperator scan = (ScanOperator) op;
            sb.append(indent).append("Scan: ").append(scan.getFilePath()).append("\n");
        } else if (op instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) op;
            sb.append(indent).append("Filter: ").append(filter.getPredicate()).append("\n");
            buildPlanString(filter.getChild(), indentLevel + 1, sb);
        } else if (op instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) op;
            sb.append(indent).append("Project: ").append(String.join(", ", project.getProjectedColumns())).append("\n");
            buildPlanString(project.getChild(), indentLevel + 1, sb);
        } else if (op instanceof JoinOperator) {
            JoinOperator join = (JoinOperator) op;
            sb.append(indent).append("Join: ").append(join.getPredicate()).append("\n");
            buildPlanString(join.getLeftChild(), indentLevel + 1, sb);
            buildPlanString(join.getRightChild(), indentLevel + 1, sb);
        } else if (op instanceof SinkOperator) {
            SinkOperator sink = (SinkOperator) op;
            sb.append(indent).append("Sink: ").append(sink.getOutputFile()).append("\n");
            buildPlanString(sink.getChild(), indentLevel + 1, sb);
        }
        else {
            sb.append(indent).append("Unknown Operator: ").append(op.getClass().getSimpleName()).append("\n");
        }
    }
}
