package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.io.*;
import java.util.List;

/**
 * DO NOT MODIFY THIS CLASS!
 *
 * The Sink operator writes the tuples output to a csv file.
 */
public class SinkOperator extends OperatorBase implements Operator {
    private Operator child;
    private String outputFile;
    private BufferedWriter writer;
    private boolean headerWritten;

    public SinkOperator(Operator child, String outputFile) {
        this.child = child;
        this.outputFile = outputFile;
        this.headerWritten = false;
    }

    @Override
    public void open() {
        logger.trace("Writing file " + outputFile);
        child.open();
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple next() {
        Tuple tuple = child.next();
        if (tuple != null) {
            try {
                if (!headerWritten) {
                    // Write header line using schema information
                    List<String> schema = tuple.getSchema();
                    writer.write(String.join(",", schema));
                    writer.newLine();
                    headerWritten = true;
                    logger.trace("Writing " + tuple.getSchema());
                }
                // Write the tuple's values as a CSV line
                List<Object> values = tuple.getValues();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < values.size(); i++) {
                    sb.append(values.get(i));
                    if (i < values.size() - 1) sb.append(",");
                }
                writer.write(sb.toString());
                writer.newLine();
                writer.flush();

                logger.trace("Writing " + sb);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tuple;
    }

    @Override
    public void close() {
        child.close();
        if (writer != null) {
            try { writer.close(); } catch (IOException e) { e.printStackTrace(); }
        }
    }

    public Operator getChild() {
        return child;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public boolean isHeaderWritten() {
        return headerWritten;
    }
}
