package in.ac.iitd.db362.operators;


import in.ac.iitd.db362.storage.Tuple;

import java.io.*;
import java.util.*;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * Scan operator reads a csv file line by line.
 */
public class ScanOperator extends OperatorBase implements Operator {
    private String filePath;
    private BufferedReader reader;
    private List<String> schema; // Column names
    private List<String> types;  // Column types (in lowercase)

    public ScanOperator(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open() {
        try {
            reader = new BufferedReader(new FileReader(filePath));
            // Read header line
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("Empty CSV file");
            }
            logger.info("Reading file " + filePath);
            String[] headerTokens = headerLine.split(",");
            schema = new ArrayList<>();
            types = new ArrayList<>();
            for (String token : headerTokens) {
                String[] parts = token.split(":");
                if (parts.length != 2) {
                    throw new IOException("Invalid header format: " + token);
                }
                schema.add(parts[0].trim());
                types.add(parts[1].trim().toLowerCase());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple next() {
        try {
            if (reader == null) return null;
            String line = reader.readLine();
            if (line == null) return null;
            String[] tokens = line.split(",");
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < tokens.length; i++) {
                String token = tokens[i].trim();
                String type = types.get(i);
                values.add(parseValue(token, type));
            }
            logger.trace("Processing line: " + values + " with schema " + schema);
            return new Tuple(values, schema);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Object parseValue(String token, String type) {
        switch (type) {
            case "integer":
                return Integer.parseInt(token);
            case "double":
                return Double.parseDouble(token);
            case "string":
                return token;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    @Override
    public void close() {
        logger.trace("close()");
        if (reader != null) {
            try { reader.close(); } catch (IOException e) { e.printStackTrace(); }
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public List<String> getSchema() {
        return schema;
    }

    public List<String> getTypes() {
        return types;
    }
}
