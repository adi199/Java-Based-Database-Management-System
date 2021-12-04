package edu.ottawa.extensions;

import java.util.logging.Logger;

/**
 * WhereConditionProcessor class This class helps with handling of logic for where clauses
 *
 * @author Team Blue
 */
public class WhereConditionProcessor {

    private static final Logger LOGGER = Logger.getLogger(WhereConditionProcessor.class.getName());

    public String columnName;
    private SupportedOperators operator;
    public String comparisonValue;
    boolean negation;
    public int columnOrdinal;
    public DBSupportedDataType dataType;

    public WhereConditionProcessor(DBSupportedDataType dataType) {
        this.dataType = dataType;
    }

    public static String[] supportedOperators = {"<=", ">=", "<>", ">", "<", "="};

    /**
     * Helps in converting the operator given by the user to SupportedOperators for processing
     *
     * @param strOperator
     * @return
     */
    public static SupportedOperators getOperatorType(String strOperator) {
        switch (strOperator) {
            case ">":
                return SupportedOperators.GREATERTHAN;
            case "<":
                return SupportedOperators.LESSTHAN;
            case "=":
                return SupportedOperators.EQUALTO;
            case ">=":
                return SupportedOperators.GREATERTHANOREQUAL;
            case "<=":
                return SupportedOperators.LESSTHANOREQUAL;
            case "<>":
                return SupportedOperators.NOTEQUAL;
            default:
                System.out.println("! Invalid operator \"" + strOperator + "\"");
                return SupportedOperators.INVALID;
        }
    }

    /**
     * Helps to perform comparison operation
     *
     * @param value1 input
     * @param value2 db value
     * @param dType  comparison data type
     * @return
     */
    public static int compare(String value1, String value2, DBSupportedDataType dType) {
        if (dType == DBSupportedDataType.TEXT)
            return value1.toLowerCase().compareTo(value2);
        else if (dType == DBSupportedDataType.NULL) {
            if (value1.equals(value2))
                return 0;
            else if (value1.toLowerCase().equals("null"))
                return -1;
            else
                return 1;
        } else {
            return Long.valueOf(Long.parseLong(value1) - Long.parseLong(value2)).intValue();
        }
    }

    /**
     * Helper for - operations
     *
     * @param operation
     * @param difference
     * @return
     */
    private boolean doOperationOnDifference(SupportedOperators operation, int difference) {
        switch (operation) {
            case LESSTHANOREQUAL:
                return difference <= 0;
            case GREATERTHANOREQUAL:
                return difference >= 0;
            case NOTEQUAL:
                return difference != 0;
            case LESSTHAN:
                return difference < 0;
            case GREATERTHAN:
                return difference > 0;
            case EQUALTO:
                return difference == 0;
            default:
                return false;
        }
    }

    /**
     * Helper for performing string comparision operations
     *
     * @param currentValue
     * @param operation
     * @return
     */
    private boolean doStringCompare(String currentValue, SupportedOperators operation) {
        return doOperationOnDifference(operation, currentValue.toLowerCase().compareTo(comparisonValue.toLowerCase()));
    }


    /**
     * Does comparison on current value  from db with the comparison value
     *
     * @param currentValue
     * @return
     */
    public boolean checkCondition(String currentValue) {
        SupportedOperators operation = getOperation();

        if (currentValue.toLowerCase().equals("null")
                || comparisonValue.toLowerCase().equals("null"))
            return doOperationOnDifference(operation, compare(currentValue, comparisonValue, DBSupportedDataType.NULL));

        if (dataType == DBSupportedDataType.TEXT || dataType == DBSupportedDataType.NULL)
            return doStringCompare(currentValue, operation);
        else {

            switch (operation) {
                case LESSTHANOREQUAL:
                    return Long.parseLong(currentValue) <= Long.parseLong(comparisonValue);
                case GREATERTHANOREQUAL:
                    return Long.parseLong(currentValue) >= Long.parseLong(comparisonValue);

                case NOTEQUAL:
                    return Long.parseLong(currentValue) != Long.parseLong(comparisonValue);
                case LESSTHAN:
                    return Long.parseLong(currentValue) < Long.parseLong(comparisonValue);

                case GREATERTHAN:
                    return Long.parseLong(currentValue) > Long.parseLong(comparisonValue);
                case EQUALTO:
                    return Long.parseLong(currentValue) == Long.parseLong(comparisonValue);

                default:
                    return false;

            }

        }

    }

    /**
     * Helper to set condition values during intermediate operations
     *
     * @param conditionValue
     */
    public void setConditionValue(String conditionValue) {
        this.comparisonValue = conditionValue;
        this.comparisonValue = comparisonValue.replace("'", "");
        this.comparisonValue = comparisonValue.replace("\"", "");

    }

    public void setColumName(String columnName) {
        this.columnName = columnName;
    }

    public void setOperator(String operator) {
        this.operator = getOperatorType(operator);
    }

    public void setNegation(boolean negate) {
        this.negation = negate;
    }

    public SupportedOperators getOperation() {
        if (!negation)
            return this.operator;
        else
            return negateOperator();
    }


    /**
     * In case of ! / NOT operation, invert the operation to be performed
     *
     * @return
     */
    private SupportedOperators negateOperator() {
        switch (this.operator) {
            case LESSTHANOREQUAL:
                return SupportedOperators.GREATERTHAN;
            case GREATERTHANOREQUAL:
                return SupportedOperators.LESSTHAN;
            case NOTEQUAL:
                return SupportedOperators.EQUALTO;
            case LESSTHAN:
                return SupportedOperators.GREATERTHANOREQUAL;
            case GREATERTHAN:
                return SupportedOperators.LESSTHANOREQUAL;
            case EQUALTO:
                return SupportedOperators.NOTEQUAL;
            default:
                System.out.println("! Invalid operator \"" + this.operator + "\"");
                return SupportedOperators.INVALID;
        }
    }
}
