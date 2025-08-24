# Query-Execution-and-Optimization-Engine

A lightweight, in-memory analytical database engine that processes CSV files using relational operators and query optimization techniques. This system implements core database concepts including the iterator model for query execution and cost-based query optimization.

## Features

### Core Relational Operators
- **Scan Operator**: Reads CSV files and converts them into tuple streams
- **Filter Operator**: Applies selection predicates with support for comparison operations (`=`, `>`, `>=`, `<`, `<=`, `!=`)
- **Project Operator**: Column projection with optional duplicate elimination
- **Hash Join Operator**: Efficient equi-join implementation using hash tables
- **Sink Operator**: Writes query results to output CSV files

### Query Optimization Engine
- **Filter Pushdown**: Moves selection operations closer to data sources
- **Join Reordering**: Uses cardinality estimation for optimal join ordering
- **Filter Merging**: Combines consecutive filter operations
- **Projection Optimization**: Merges and optimizes projection operations
- **Cost-Based Decisions**: Leverages statistical metadata for optimization choices

### Fluent API
Provides an intuitive query builder interface for constructing analytical queries:

```java
Operator plan = PlanBuilder.scan("customers.csv")
    .filter("age > 30")
    .join(PlanBuilder.scan("orders.csv"), "customer_id = order_customer_id")
    .project("name", "order_total")
    .sink("results.csv")
    .build();
```

## Architecture

### Iterator Model
All operators implement a standard interface with three key methods:
- `open()`: Initialize operator resources and state
- `next()`: Return the next tuple in the result stream
- `close()`: Clean up resources and finalize processing

### Statistical Catalog
The system maintains comprehensive metadata about data sources:
- Table-level statistics (row count, column count)
- Column-level statistics (min/max values, cardinality, histograms)
- Used by the query optimizer for cost estimation and plan selection

### CSV Data Format
Tables are represented as CSV files with typed schemas:
- First row contains column definitions: `columnName:dataType`
- Supported types: `integer`, `string`, `double`
- Unique attribute names across all tables

## Implementation Highlights

### Hash Join Algorithm
```java
// Build phase: Hash smaller relation
Map<Object, List<Tuple>> hashTable = new HashMap<>();
while ((leftTuple = leftChild.next()) != null) {
    // Hash on join attributes
    hashTable.computeIfAbsent(joinKey, k -> new ArrayList<>()).add(leftTuple);
}

// Probe phase: Match tuples from larger relation
while ((rightTuple = rightChild.next()) != null) {
    List<Tuple> matches = hashTable.get(probeKey);
    // Return matching joined tuples
}
```

### Query Optimization Strategy
1. **Rule-Based Optimizations**
   - Push filters through joins when they reference only one relation
   - Merge consecutive operations where possible
   - Eliminate redundant projections

2. **Cost-Based Optimizations**
   - Estimate relation cardinalities using catalog statistics
   - Apply selectivity estimates for predicates (default: 30% for comparisons)
   - Reorder joins to process smaller relations first

### Predicate Evaluation
Supports complex predicate evaluation with type coercion:
```java
// Handle different data types
if (leftValue instanceof Number && rightValue instanceof Number) {
    leftValue = ((Number) leftValue).doubleValue();
    rightValue = ((Number) rightValue).doubleValue();
}

// Apply comparison operator
switch (operator) {
    case "=": return compareResult == 0;
    case ">": return compareResult > 0;
    // ... other operators
}
```

## Project Structure

```
src/main/java/
├── operators/           # Core relational operators
│   ├── ScanOperator.java
│   ├── FilterOperator.java
│   ├── ProjectOperator.java
│   ├── JoinOperator.java
│   └── SinkOperator.java
├── predicates/          # Query predicate implementations
│   ├── ComparisonPredicate.java
│   └── EqualityJoinPredicate.java
├── optimizer/           # Query optimization engine
│   └── BasicOptimizer.java
├── api/                 # Query builder and execution
│   └── PlanBuilder.java
└── catalog/             # Statistical metadata management
    └── Catalog.java
```

## Usage Examples

### Basic Filtering and Projection
```java
Operator plan = PlanBuilder.scan("employees.csv")
    .filter("department = engineering")
    .filter("salary > 75000")
    .project("name", "salary")
    .sink("high_paid_engineers.csv")
    .build();

QueryExecutor.execute(plan);
```

### Multi-Table Joins
```java
Operator plan = PlanBuilder.scan("customers.csv")
    .join(PlanBuilder.scan("orders.csv"), "customer_id = order_customer_id")
    .join(PlanBuilder.scan("products.csv"), "product_id = order_product_id")
    .filter("order_date > 2024-01-01")
    .project("customer_name", "product_name", "order_total")
    .sink("recent_orders.csv")
    .build();
```

### Query Optimization
```java
// Initialize catalog with statistics
Catalog catalog = new Catalog();
DataLoader.createStatistics("table1.csv", catalog);
DataLoader.createStatistics("table2.csv", catalog);

// Build and optimize query plan
Operator plan = PlanBuilder.scan("table1.csv")
    .join(PlanBuilder.scan("table2.csv"), "id = foreign_id")
    .filter("status = active")
    .build();

Optimizer optimizer = new BasicOptimizer(catalog);
Operator optimizedPlan = optimizer.optimize(plan);

QueryExecutor.execute(optimizedPlan);
```

## Technical Specifications

- **Language**: Java 11+
- **Build Tool**: Maven
- **Memory Management**: In-memory processing with iterator-based streaming
- **Join Algorithm**: Hash-based equi-joins
- **Optimization**: Rule-based and cost-based hybrid approach
- **Data Types**: Integer, String, Double with automatic type coercion

## Performance Characteristics

- **Join Performance**: O(n + m) hash join for equi-joins
- **Memory Usage**: Hash table size proportional to smaller relation
- **Optimization**: Linear time complexity for most optimization rules
- **Scalability**: Limited by available memory for hash tables

## Future Enhancements

- Support for more join algorithms (sort-merge, nested loop)
- Advanced statistics collection (column correlations, data skew)
- Parallel query execution
- Support for additional data types and operations
- Integration with external data sources
- Query result caching and materialized views

## Build and Test

```bash
# Build the project
mvn clean install -DskipTests

# Run tests
mvn test

# Execute sample queries
java -cp target/classes QueryTest
```

This implementation demonstrates fundamental database system concepts including query processing, optimization, and execution in a clean, educational codebase suitable for understanding relational database internals.
