package in.ac.iitd.db362.query;

import in.ac.iitd.db362.api.PlanBuilder;
import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.executor.QueryExecutor;
import in.ac.iitd.db362.operators.Operator;
import in.ac.iitd.db362.optimizer.BasicOptimizer;
import in.ac.iitd.db362.optimizer.Optimizer;
import in.ac.iitd.db362.storage.DataLoader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class test {

private static final String CUSTOMER_CSV = "data/csvTables/large_customer.csv";
private static final String ORDERS_CSV = "data/csvTables/large_orders.csv";
private static final String PRODUCTS_CSV = "data/csvTables/large_products.csv";
private static final String SUPPLIER_CSV = "data/csvTables/large_supplier.csv";
private static final String REGION_CSV = "data/csvTables/large_region.csv";


public static void testJoinAndFilter() throws Exception {
    // Step 1: Build original plan
    Operator originalPlan = PlanBuilder.scan(CUSTOMER_CSV)
            .join(PlanBuilder.scan(ORDERS_CSV), "c_customer_id = o_customer_id")
            .filter("c_age > 30")
            .project("c_name", "c_age", "o_order_id")
            .sink("data/output/output_original.csv")
            .build();



    // Step 3: Initialize catalog and statistics
    Catalog catalog = new Catalog();
    DataLoader.createStatistics(CUSTOMER_CSV, catalog);
    DataLoader.createStatistics(ORDERS_CSV, catalog);

    long startOptimized = System.currentTimeMillis();

    Optimizer optimizer = new BasicOptimizer(catalog);
    Operator optimizedPlan = optimizer.optimize(originalPlan);

    QueryExecutor.execute(optimizedPlan);
    long optimizedTime = System.currentTimeMillis() - startOptimized;

    // Step 2: Execute original plan and time it
    long startOriginal = System.currentTimeMillis();
    QueryExecutor.execute(originalPlan);
    long originalTime = System.currentTimeMillis() - startOriginal;

    // Step 6: Print results
    System.out.println("\nPerformance Comparison:");
    System.out.println("Original Plan Time: " + originalTime + "ms");
    System.out.println("Optimized Plan Time: " + optimizedTime + "ms");
    System.out.println("Improvement: " + (originalTime - optimizedTime) + "ms");

    // Step 7: Validate outputs
    // assertTrue(Files.exists(Paths.get("data/output/output_original.csv")));
}

public static void testComplexPlan() throws Exception {
    // Build inefficient plan
    Operator originalPlan = PlanBuilder.scan(CUSTOMER_CSV)
            .join(PlanBuilder.scan(ORDERS_CSV), "c_customer_id = o_customer_id")
            .join(PlanBuilder.scan(PRODUCTS_CSV), "o_product_id = p_product_id")
            .join(PlanBuilder.scan(SUPPLIER_CSV), "p_product_id = s_supplier_id")
            .join(PlanBuilder.scan(REGION_CSV), "s_region_id = r_region_id")
            .filter("c_age > 30")
            .filter("p_price < 100")
            .filter("s_rating > 3")
            .project("c_name", "o_order_id", "p_name", "s_name", "r_name")
            .sink("data/output/complex_original.csv")
            .build();



    // Optimize
    Catalog catalog = new Catalog();
    DataLoader.createStatistics(CUSTOMER_CSV, catalog);
    DataLoader.createStatistics(ORDERS_CSV, catalog);
    DataLoader.createStatistics(PRODUCTS_CSV, catalog);
    DataLoader.createStatistics(SUPPLIER_CSV, catalog);
    DataLoader.createStatistics(REGION_CSV, catalog);

    // Execute original plan
    long start = System.currentTimeMillis();
    QueryExecutor.execute(originalPlan);
    long originalTime = System.currentTimeMillis() - start;

    // Execute optimized plan
    start = System.currentTimeMillis();
    Optimizer optimizer = new BasicOptimizer(catalog);
    Operator optimizedPlan = optimizer.optimize(originalPlan);
    QueryExecutor.execute(optimizedPlan);
    long optimizedTime = System.currentTimeMillis() - start;

    System.out.println("\nPerformance Comparison:");
    System.out.println("Original Plan: " + originalTime + "ms");
    System.out.println("Optimized Plan: " + optimizedTime + "ms");
    System.out.println("Improvement: " + (originalTime - optimizedTime) + "ms");

    // assertTrue(Files.exists(Paths.get("data/output/complex_original.csv")));
}


private static void testVeryInefficientPlan() throws Exception {
    String outputFile = "data/output/very_inefficient_original.csv";

    // Step 1: Build a poorly structured plan (filters applied late, joins early)
    Operator originalPlan = PlanBuilder
            .scan(SUPPLIER_CSV) // join supplier first for no good reason
            .join(PlanBuilder.scan(PRODUCTS_CSV), "s_supplier_id = p_product_id")
            .project("s_name", "p_name", "s_region_id", "p_product_id") // early projection, not helpful yet
            .join(PlanBuilder.scan(ORDERS_CSV), "p_product_id = o_product_id")
            .join(PlanBuilder.scan(CUSTOMER_CSV), "o_customer_id = c_customer_id")
            .join(PlanBuilder.scan(REGION_CSV), "s_region_id = r_region_id")
            .filter("p_price < 100") // late filters
            .filter("c_age > 30")
            .filter("s_rating > 3")
            .project("c_name", "o_order_id", "p_name", "s_name", "r_name")
            .sink(outputFile)
            .build();

    // Step 2: Execute the original bad plan
    long startBad = System.currentTimeMillis();
    QueryExecutor.execute(originalPlan);
    long badTime = System.currentTimeMillis() - startBad;

    // Step 3: Setup catalog and optimizer
    Catalog catalog = new Catalog();
    DataLoader.createStatistics(CUSTOMER_CSV, catalog);
    DataLoader.createStatistics(ORDERS_CSV, catalog);
    DataLoader.createStatistics(PRODUCTS_CSV, catalog);
    DataLoader.createStatistics(SUPPLIER_CSV, catalog);
    DataLoader.createStatistics(REGION_CSV, catalog);

    Optimizer optimizer = new BasicOptimizer(catalog);
    Operator optimizedPlan = optimizer.optimize(originalPlan);

    // Step 4: Execute optimized plan
    long startGood = System.currentTimeMillis();
    QueryExecutor.execute(optimizedPlan);
    long goodTime = System.currentTimeMillis() - startGood;

    // Step 5: Results
    System.out.println("\nPerformance Comparison for Very Inefficient Plan:");
    System.out.println("Original Plan Time: " + badTime + "ms");
    System.out.println("Optimized Plan Time: " + goodTime + "ms");
    System.out.println("Improvement: " + (badTime - goodTime) + "ms");

    // Step 6: Assert output exists
    // assertTrue(Files.exists(Paths.get(outputFile)));
}

public static void main(String[] args) throws Exception {
    testJoinAndFilter();
    testComplexPlan();
//    testVeryInefficientPlan();
}
}