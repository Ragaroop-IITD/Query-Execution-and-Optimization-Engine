package in.ac.iitd.db362.storage;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.catalog.DoubleColumnStatistics;
import in.ac.iitd.db362.catalog.IntColumnStatistics;
import in.ac.iitd.db362.catalog.StringColumnStatistics;

import in.ac.iitd.db362.catalog.TableStatistics;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * Read a CSV file and populate the catalog. Particularly, for each column, based on its type
 * it creates column statistics and stores it in the catalog.
 */
public class DataLoader {

    protected final static Logger logger = LogManager.getLogger();

    /**
     * Read a CSV file and create column statistics
     * The header must be in the format: attributeName:attributeType,
     * e.g., "age:integer,name:string,salary:double"
     *
     * @param filePath the CSV file path
     * @throws IOException if file reading fails or header is invalid
     */
    public static void createStatistics(String filePath, Catalog catalog) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                throw new IOException("Empty CSV file");
            }

            // Process header to extract column names and types
            String[] headerTokens = headerLine.split(",");
            int numColumns = headerTokens.length;
            Schema schema = new Schema();
            // Create a table using the file name as tableName (or use another naming convention)
            Table table = new Table(filePath, schema);

            String[] colNames = new String[numColumns];
            String[] colTypes = new String[numColumns];

            for (int i = 0; i < numColumns; i++) {
                String[] parts = headerTokens[i].split(":");
                if (parts.length != 2) {
                    throw new IOException("Invalid header format. Expected attributeName:attributeType");
                }
                colNames[i] = parts[0].trim();
                colTypes[i] = parts[1].trim().toLowerCase();
                schema.addColumnMeta(new ColumnMeta(colNames[i], colTypes[i]));
                catalog.registerColumn(colNames[i], table.getTableName());
            }

            // Prepare storage for raw data per column
            List<List<String>> rawColumns = new ArrayList<>();
            for (int i = 0; i < numColumns; i++) {
                rawColumns.add(new ArrayList<>());
            }

            // Read each line and split into tokens
            int numRows = 0;
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");
                for (int i = 0; i < numColumns; i++) {
                    rawColumns.get(i).add(tokens[i].trim());
                }
                numRows++;
            }



            // Create a TableStatistics object to accumulate column statistics
            TableStatistics tableStats = new TableStatistics(numRows);



            // Process each column based on its declared type and add it to the table
            IntList intData = new IntArrayList();
            DoubleList doubleData = new DoubleArrayList();

            for (int i = 0; i < numColumns; i++) {
                String colName = colNames[i];
                String colType = colTypes[i];
                List<String> rawData = rawColumns.get(i);

                switch (colType) {
                    case "integer":
                        for (String s : rawData) {
                            intData.add(Integer.parseInt(s));
                        }
//                        IntCompressedColumn intCol = new IntCompressedColumn();
//                        intCol.loadData(intData);
//                        table.addColumn(colName, intCol);
                        tableStats.addColumnStatistics(colName, new IntColumnStatistics(intData));
                        intData.clear();

                        logger.info("Created column statistics for " + colName + " of type " + colType);
                        break;
                    case "double":

                        for (String s : rawData) {
                            doubleData.add(Double.parseDouble(s));
                        }
                        tableStats.addColumnStatistics(colName, new DoubleColumnStatistics(doubleData));
                        doubleData.clear();
                        logger.info("Created column statistics for " + colName + " of type " + colType);
                        break;
                    case "string":
                        tableStats.addColumnStatistics(colName, new StringColumnStatistics(rawData));
                        logger.info("Created column statistics for " + colName + " of type " + colType);
                        break;
                    default:
                        throw new IOException("Unsupported column type: " + colType);
                }
            }
            // Register the computed statistics in the Catalog using the table's name
            catalog.registerTable(table.getTableName(), tableStats);
            logger.info("Created table statistics for " + table.getTableName());
        }
    }
}
