package edu.ottawa.extensions;

import java.util.logging.Logger;

/**
 * WhereConditionProcessor class This class helps with handling of logic for where clauses
 *
 * @author Team Ottawa
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
     * convert the operator given by the user to SupportedOperators for processing
     *
     * @param strOperator
     * @return
     */
    public static SupportedOperators getOperatorType(String strOperator) {
        switch (strOperator) {
            case ">":
                return SupportedOperators.GREATER_THAN;
            case "<":
                return SupportedOperators.LESS_THAN;
            case "=":
                return SupportedOperators.EQUAL_TO;
            case ">=":
                return SupportedOperators.GREATER_THAN_OR_EQUAL;
            case "<=":
                return SupportedOperators.LESS_THAN_OR_EQUAL;
            case "<>":
                return SupportedOperators.NOT_EQUAL;
            default:
                System.out.println("! Invalid operator \"" + strOperator + "\"");
                return SupportedOperators.INVALID;
        }
    }

    /**
     * perform comparison operation
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
     * Handle operations
     *
     * @param operation
     * @param difference
     * @return
     */
    private boolean doOperationOnDifference(SupportedOperators operation, int difference) {
        switch (operation) {
            case LESS_THAN_OR_EQUAL:
                return difference <= 0;
            case GREATER_THAN_OR_EQUAL:
                return difference >= 0;
            case NOT_EQUAL:
                return difference != 0;
            case LESS_THAN:
                return difference < 0;
            case GREATER_THAN:
                return difference > 0;
            case EQUAL_TO:
                return difference == 0;
            default:
                return false;
        }
    }

    /**
     * performs string comparison operations
     *
     * @param currentValue
     * @param operation
     * @return
     */
    private boolean doStringCompare(String currentValue, SupportedOperators operation) {
        return doOperationOnDifference(operation, currentValue.toLowerCase().compareTo(comparisonValue.toLowerCase()));
    }


    /**
     * Does comparison on current value from db with the comparison value
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
                case LESS_THAN_OR_EQUAL:
                    return Long.parseLong(currentValue) <= Long.parseLong(comparisonValue);
                case GREATER_THAN_OR_EQUAL:
                    return Long.parseLong(currentValue) >= Long.parseLong(comparisonValue);

                case NOT_EQUAL:
                    return Long.parseLong(currentValue) != Long.parseLong(comparisonValue);
                case LESS_THAN:
                    return Long.parseLong(currentValue) < Long.parseLong(comparisonValue);

                case GREATER_THAN:
                    return Long.parseLong(currentValue) > Long.parseLong(comparisonValue);
                case EQUAL_TO:
                    return Long.parseLong(currentValue) == Long.parseLong(comparisonValue);

                default:
                    return false;

            }

        }

    }

    /**
     * set condition values during intermediate operations
     *
     * @param conditionValue
     */
    public void setConditionValue(String conditionValue) {
        this.comparisonValue = conditionValue;
        this.comparisonValue = comparisonValue.replace("'", "");
        this.comparisonValue = comparisonValue.replace("\"", "");

    }

    public void setColumnName(String columnName) {
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
            case LESS_THAN_OR_EQUAL:
                return SupportedOperators.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return SupportedOperators.LESS_THAN;
            case NOT_EQUAL:
                return SupportedOperators.EQUAL_TO;
            case LESS_THAN:
                return SupportedOperators.GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return SupportedOperators.LESS_THAN_OR_EQUAL;
            case EQUAL_TO:
                return SupportedOperators.NOT_EQUAL;
            default:
                System.out.println("! Invalid operator \"" + this.operator + "\"");
                return SupportedOperators.INVALID;
        }
    }
}
