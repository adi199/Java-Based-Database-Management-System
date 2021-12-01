package edu.blue;

import edu.blue.extensions.DBOperationsProcessor;
import edu.blue.extensions.DBSupportedDataType;
import edu.blue.extensions.WhereConditionProcessor;
import edu.blue.extensions.TableInfoHandler;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Team Blue - Partial Author
 */
public class Utils {

    /**
     * Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(printSeparator("-", 80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
        System.out.println("DavisBaseLite Version " + Settings.getVersion());
        System.out.println(Settings.getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(printSeparator("-", 80));
    }

    public static String printSeparator(String s, int len) {
        String bar = "";
        for (int i = 0; i < len; i++) {
            bar += s;
        }
        return bar;
    }

    /**
     * Helper method that return default path where tables are stored if present
     *
     * @param tableName
     * @return
     */
    public static String getTableFilePath(String tableName) {
        if (tableName.equals(DBOperationsProcessor.tablesTable) || tableName.equals(DBOperationsProcessor.columnsTable))
            return "data/" + tableName + ".tbl";
        return "data/user_data/" + tableName + ".tbl";
    }

    /**
     * Helper method that returns the path where indexes are stored and new one should be created
     *
     * @param tableName
     * @param columnName
     * @return
     */
    public static String getIndexFilePath(String tableName, String columnName) {
        if (tableName.equals(DBOperationsProcessor.tablesTable) || tableName.equals(DBOperationsProcessor.columnsTable))
            return "data/" + tableName + "_" + columnName + ".ndx";

        return "data/user_data/" + tableName + "_" + columnName + ".ndx";
    }

    public static WhereConditionProcessor getConditionFromCurrentQuery(TableInfoHandler tableMetaData, String query) throws Exception {
        if (query.contains("where")) {
            WhereConditionProcessor condition = new WhereConditionProcessor(DBSupportedDataType.TEXT);
            String whereCondition = query.substring(query.indexOf("where") + 6);
            ArrayList<String> whereClauseTokens = new ArrayList<>(Arrays.asList(whereCondition.split(" ")));

            if (whereClauseTokens.get(0).equalsIgnoreCase("not")) {
                condition.setNegation(true);
            }


            for (int i = 0; i < WhereConditionProcessor.supportedOperators.length; i++) {
                if (whereCondition.contains(WhereConditionProcessor.supportedOperators[i])) {
                    whereClauseTokens = new ArrayList<>(
                            Arrays.asList(whereCondition.split(WhereConditionProcessor.supportedOperators[i])));
                    {
                        condition.setOperator(WhereConditionProcessor.supportedOperators[i]);
                        condition.setConditionValue(whereClauseTokens.get(1).trim());
                        condition.setColumName(whereClauseTokens.get(0).trim());
                        break;
                    }

                }
            }


            if (tableMetaData.tableExists && tableMetaData.columnExists(new ArrayList<String>(Arrays.asList(condition.columnName)))) {
                condition.columnOrdinal = tableMetaData.columnNamesList.indexOf(condition.columnName);
                condition.dataType = tableMetaData.columnNameAttrList.get(condition.columnOrdinal).dataType;

                if (condition.dataType != DBSupportedDataType.TEXT && condition.dataType != DBSupportedDataType.NULL) {
                    try {
                        Long.parseLong(condition.comparisonValue);
                    } catch (Exception e) {
                        throw new Exception("Invalid/Non supported Comparison");
                    }
                }

            } else {
                throw new Exception("Invalid Table/Column : " + tableMetaData.tableName + " . " + condition.columnName);
            }
            return condition;
        } else
            return null;
    }

}
