package in.ac.iitd.db362.operators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * DO NOT CHANGE THIS FILE!
 *
 * Base class to share logger among all operators!
 */
public abstract class OperatorBase implements Operator{
    protected final Logger logger = LogManager.getLogger(this.getClass());
}
