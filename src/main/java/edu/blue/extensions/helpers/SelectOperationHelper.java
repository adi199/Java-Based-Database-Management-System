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
 * This helper class is used to handle select operations
 *
 * @author Team Ottawa
 */
public class SelectOperationHelper {

    private static final Logger logger = Logger.getLogger(SelectOperationHelper.class.getName());

    /**
     * This method is used to handle select operations
     *
     * @param inputString
     */
    public static void handleSelectOperation(String inputString) {
        String tableName = "";
        List<String> columnList = new ArrayList<>();

        // Get table and column names for the selection operation
        ArrayList<String> tableTokenList = new ArrayList<>(Arrays.asList(inputString.split(" ")));
        int i;

        for (i = 1; i < tableTokenList.size(); i++) {
            if (tableTokenList.get(i).equals("from")) {
                ++i;
                tableName = tableTokenList.get(i);
                break;
            }
            if (!tableTokenList.get(i).equals("*") && !tableTokenList.get(i).equals(",")) {
                if (tableTokenList.get(i).contains(",")) {
                    ArrayList<String> columns = new ArrayList<>(
                            Arrays.asList(tableTokenList.get(i).split(",")));
                    for (String columnRecord : columns) {
                        columnList.add(columnRecord.trim());
                    }
                } else
                    columnList.add(tableTokenList.get(i));
            }
        }

        TableInfoHandler tableMetaData = new TableInfoHandler(tableName);
        if (!tableMetaData.tableExists) {
            System.out.println("Table does not exist.");
            return;
        }

        if (!tableMetaData.columnExists(columnList)) {
            System.out.println("Invalid column name(s).");
            return;
        }

        WhereConditionProcessor condition;
        try {

            condition = getConditionFromCurrentQuery(tableMetaData, inputString);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        if (columnList.size() == 0) {
            columnList = tableMetaData.columnNamesList;
        }
        try {

            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "r");
            DBOperationsProcessor tableBinaryFile = new DBOperationsProcessor(tableFile);
            tableBinaryFile.selectRecordsFromTable(tableMetaData, columnList, condition);
            tableFile.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception while retrieving the columns", e);
            System.out.println("Error while selecting columns from table");
        }

    }

}
