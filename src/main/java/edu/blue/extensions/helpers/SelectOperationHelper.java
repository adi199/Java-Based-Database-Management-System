package edu.blue.extensions.helpers;

import edu.blue.extensions.DBOperationsProcessor;
import edu.blue.extensions.WhereConditionProcessor;
import edu.blue.extensions.TableInfoHandler;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.blue.Utils.getConditionFromCurrentQuery;
import static edu.blue.Utils.getTableFilePath;

/**
 * This helper class is used for performing select operations
 *
 * @author Team Blue
 */
public class SelectOperationHelper {

    private static final Logger LOGGER = Logger.getLogger(SelectOperationHelper.class.getName());

    /**
     * This method is used for performing select operations
     *
     * @param queryString
     */
    public static void performSelectOperation(String queryString) {
        String tableName = "";
        List<String> columnNames = new ArrayList<>();

        // Get table and column names for the select
        ArrayList<String> tableTokens = new ArrayList<>(Arrays.asList(queryString.split(" ")));
        int i;

        for (i = 1; i < tableTokens.size(); i++) {
            if (tableTokens.get(i).equals("from")) {
                ++i;
                tableName = tableTokens.get(i);
                break;
            }
            if (!tableTokens.get(i).equals("*") && !tableTokens.get(i).equals(",")) {
                if (tableTokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<>(
                            Arrays.asList(tableTokens.get(i).split(",")));
                    for (String col : colList) {
                        columnNames.add(col.trim());
                    }
                } else
                    columnNames.add(tableTokens.get(i));
            }
        }

        TableInfoHandler tableMetaData = new TableInfoHandler(tableName);
        if (!tableMetaData.tableExists) {
            System.out.println("Table does not exist!");
            return;
        }

        if (!tableMetaData.columnExists(columnNames)) {
            System.out.println("Invalid column name(s)!");
            return;
        }

        WhereConditionProcessor condition;
        try {

            condition = getConditionFromCurrentQuery(tableMetaData, queryString);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        if (columnNames.size() == 0) {
            columnNames = tableMetaData.columnNamesList;
        }
        try {

            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "r");
            DBOperationsProcessor tableBinaryFile = new DBOperationsProcessor(tableFile);
            tableBinaryFile.selectRecordsFromTable(tableMetaData, columnNames, condition);
            tableFile.close();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception in retrieving the columns", e);
            System.out.println("! Error selecting columns from table");
        }

    }

}
