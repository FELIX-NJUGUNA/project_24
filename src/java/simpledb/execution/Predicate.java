package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */

   // The field number of the tuple to compare against
    private int field;

    // The operator to use for comparison
    private Op op;

    // The operand to compare the tuple field to
    private Field operand;

    // Constructor for Predicate
    public Predicate(int field, Op op, Field operand) {
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    // Method to get the field number
    public int getField() {
        return field;
    }

    // Method to get the operator
    public Op getOp() {
        return op;
    }

    // Method to get the operand
    public Field getOperand() {
        return operand;
    }

    // Method to filter a tuple based on the predicate
    public boolean filter(Tuple t) {
        Field fieldValue = t.getField(this.field);
        int comparisonResult = fieldValue.compare(op, operand);

        switch (op) {
            case EQUALS:
                return comparisonResult== 0;
            case GREATER_THAN:
                return comparisonResult > 0;
            case LESS_THAN:
                return comparisonResult < 0;
            case LESS_THAN_OR_EQ:
                return comparisonResult <= 0;
            case GREATER_THAN_OR_EQ:
                return comparisonResult >= 0;
            case LIKE:
                // Implementation of LIKE operator is not provided
                throw new UnsupportedOperationException("LIKE operator is not supported");
            case NOT_EQUALS:
                return comparisonResult != 0;
            default:
                throw new IllegalStateException("impossible to reach here");
        }
    }

    
   
    // Method to get a string representation of the predicate
    public String toString() {
        return "f = " + field + " op = " + op.toString() + " operand = " + operand.toString();
    }
}
