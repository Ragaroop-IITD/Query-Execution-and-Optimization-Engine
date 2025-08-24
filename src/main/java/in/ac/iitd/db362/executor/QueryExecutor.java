package in.ac.iitd.db362.executor;

import in.ac.iitd.db362.api.PlanPrinter;
import in.ac.iitd.db362.operators.Operator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * Executes a given query plan using pipelining. Each operator implements the operator interface
 */
public class QueryExecutor {

    protected final static Logger logger = LogManager.getLogger();


    /**
     * Executes a given query plan.
     *
     * @param plan The root operator of the query plan
     */
    public static void execute(Operator plan) {
        logger.info("Executing plan\n" + PlanPrinter.getPlanString(plan));
        plan.open();
        while (plan.next() != null) {
            // The processing happens via pipelining
            // so nothing to do here
        }
        plan.close();
    }
}
