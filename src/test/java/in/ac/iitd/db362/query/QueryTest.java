package in.ac.iitd.db362.query;

import in.ac.iitd.db362.api.PlanBuilder;
import in.ac.iitd.db362.executor.QueryExecutor;
import in.ac.iitd.db362.operators.Operator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

//@Disabled
public class QueryTest {

    @BeforeAll
    public static void setup() {
        Configurator.setLevel("in.ac.iitd.db362.operators", Level.INFO);
        Configurator.setLevel("in.ac.iitd.db362.optimizer", Level.INFO);
        Configurator.setLevel("in.ac.iitd.db362.executor.QueryExecutor", Level.INFO);
    }

    @Test
    public void testCustomerFilter() throws Exception {

        // Output file
        String outputFile = "data/output/test_customer_filter_output.csv";

        // Build a simple plan for
        // select name, age from Customer where age > 30
        Operator plan = PlanBuilder.scan("data/csvTables/customer.csv")
                .filter("c_age > 30")
                .project("c_name", "c_age")
                .sink(outputFile)
                .build();

        // execute the plan
        QueryExecutor.execute(plan);

        String expected =
                "c_name,c_age\n" +
                        "Mark,40\n" +
                        "Alice,35\n" +
                        "Tom,45\n" +
                        "Sara,33\n" +
                        "Linda,31\n";
        assertTrue(compareFiles(outputFile, expected, "testCustomerFilter"));
    }


    // Test Case 2: Join Customer and Orders, Project Customer name and order_id.
    // Query: SELECT name, order_id FROM Customer JOIN Orders ON customer_id = customer_id
    @Test
    public void testCustomerOrdersJoin() throws Exception {
        String outputFile = "data/output/test_customer_orders_join_output.csv";

        // Build join plan: first scan Customer then join with Orders on customer_id
        Operator plan = PlanBuilder.scan("data/csvTables/customer.csv")
                .join(
                        PlanBuilder.scan("data/csvTables/orders.csv"),
                        "c_customer_id = o_customer_id")
                .project("c_name", "o_order_id")
                .sink(outputFile)
                .build();

        // Execute the plan
        QueryExecutor.execute(plan);

        String expected =
                "c_name,o_order_id\n" +
                        "John,101\n" +
                        "John,103\n" +
                        "Jane,102\n" +
                        "Jane,107\n" +
                        "Mark,104\n" +
                        "Alice,110\n" +
                        "Bob,105\n" +
                        "Tom,106\n" +
                        "Sara,108\n" +
                        "Linda,109\n";
        assertTrue(compareFiles(outputFile, expected, "testCustomerOrdersJoin"));
    }


    @Test
    // Test Case 3: Join Orders and Product, Project order_id and product_name.
    // Query: SELECT order_id, product_name FROM Orders JOIN Product ON product_id = product_id
    public void testOrdersProductJoin() throws Exception {

        String outputFile = "data/output/test_orders_product_join_output.csv";

        Operator plan = PlanBuilder.scan("data/csvTables/orders.csv")
                .join(PlanBuilder.scan("data/csvTables/product.csv"), "o_product_id = p_product_id")
                .project("o_order_id", "p_product_name")
                .sink(outputFile)
                .build();

        QueryExecutor.execute(plan);

        String expected =
                "o_order_id,p_product_name\n" +
                        "101,Widget\n" +
                        "102,Gadget\n" +
                        "103,Thingamajig\n" +
                        "104,Doohickey\n" +
                        "105,Contraption\n" +
                        "106,Device\n" +
                        "107,Apparatus\n" +
                        "108,Instrument\n" +
                        "109,Tool\n" +
                        "110,Machine\n";
        assertTrue(compareFiles(outputFile, expected, "testOrdersProductJoin"));
    }

    @Test
    public void testCustomerOrdersProductJoin() throws Exception {
        String outputFile = "data/output/test_customer_orders_product_join_output.csv";

        // Build the plan:
        // 1. Scan Customer.csv and filter customers with age > 30.
        // 2. Join with Orders.csv on customer_id.
        // 3. Join with Product.csv on product_id.
        // 4. Project the customer's name and the product name.
        // 5. Sink the result to an output CSV file.
        Operator plan = PlanBuilder.scan("data/csvTables/customer.csv")
                .filter("c_age > 30")
                .join(PlanBuilder.scan("data/csvTables/orders.csv"), "c_customer_id = o_customer_id")
                .join(PlanBuilder.scan("data/csvTables/product.csv"), "o_product_id = p_product_id")
                .project("c_name", "p_product_name")
                .sink(outputFile)
                .build();

        // Execute the plan (QueryExecutor.execute does not print tuples since SinkOperator writes output)
        QueryExecutor.execute(plan);

        // Define expected CSV output (including header).
        String expected =
                "c_name,p_product_name\n" +
                        "Mark,Doohickey\n" +
                        "Alice,Machine\n" +
                        "Tom,Device\n" +
                        "Sara,Instrument\n" +
                        "Linda,Tool\n";

        // Compare the output CSV file with the expected result.
        assertTrue(compareFiles(outputFile, expected, "testCustomerOrdersProductJoin"));
    }

    @Test
    public void testCustomerOrdersProductJoinWithPriceFilter() throws Exception {
        String outputFile = "data/output/test_customer_orders_product_join_with_price_filter_output.csv";

        // Build the plan:
        // 1. Scan Customer.csv and filter customers with age > 30.
        // 2. Join with Orders.csv on customer_id.
        // 3. Join with Product.csv on product_id.
        // 4. Apply an additional filter: only keep rows where price > 20.
        // 5. Project the customer's name and the product name.
        // 6. Sink the result to an output CSV file.
        Operator plan = PlanBuilder.scan("data/csvTables/customer.csv")
                .join(PlanBuilder.scan("data/csvTables/orders.csv"), "c_customer_id = o_customer_id")
                .join(PlanBuilder.scan("data/csvTables/product.csv"), "o_product_id = p_product_id")
                .filter("p_price > 20")
                .filter("c_age > 30")
                .project("c_name", "p_product_name")
                .sink(outputFile)
                .build();

        // Execute the plan.
        QueryExecutor.execute(plan);

        // Define expected CSV output (including header).
        String expected =
                "c_name,p_product_name\n" +
                        "Alice,Machine\n" +
                        "Tom,Device\n" +
                        "Sara,Instrument\n";

        assertTrue(compareFiles(outputFile, expected, "testCustomerOrdersProductJoinWithPriceFilter"));
    }


    // -------------------------------
    // Helper: Compare output file with expected content.
    private static boolean compareFiles(String outputFile, String expected, String testName) throws Exception {

        List<String> outputLines = Files.readAllLines(Paths.get(outputFile));

        StringBuilder outputBuilder = new StringBuilder();
        for (String line : outputLines) {
            outputBuilder.append(line).append("\n");
        }

        String output = outputBuilder.toString();

        return true ? output.equals(expected) : false;
    }
}
