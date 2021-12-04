package edu.ottawa.extensions.helpers;

import edu.ottawa.extensions.*;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ottawa.Utils.getIndexFilePath;
import static edu.ottawa.Utils.getTableFilePath;

/**
 * This helper class is used to handle insert operation on the tables
 *
 * @author Team Ottawa
 */
public class InsertOperationHelper {

    private static final Logger logger = Logger.getLogger(InsertOperationHelper.class.getName());

    /**
     * This method handles the insertion operation
     *
     * @param inputQueryString
     */
    public static void handleInsertOperation(String inputQueryString) {
        ArrayList<String> insertTokenList = new ArrayList<String>(Arrays.asList(inputQueryString.split(" ")));

        if (!insertTokenList.get(1).equals("into") || !inputQueryString.contains(") values")) {
            System.out.println("Invalid Syntax!");
            return;
        }

        try {
            String tableName = insertTokenList.get(2);
            if (tableName.trim().length() == 0) {
                System.out.println("Table name cannot be empty!");
                return;
            }

            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }
            TableInfoHandler tableMetaData = new TableInfoHandler(tableName);

            if (!tableMetaData.tableExists) {
                System.out.println("Table does not exist!");
                return;
            }

            ArrayList<String> columnTokenList = new ArrayList<String>(Arrays.asList(
                    inputQueryString.substring(inputQueryString.indexOf("(") + 1, inputQueryString.indexOf(") values")).split(",")));

            // Column List validation
            for (String colTokenRecord : columnTokenList) {
                if (!tableMetaData.columnNamesList.contains(colTokenRecord.trim())) {
                    System.out.println("Invalid column : " + colTokenRecord.trim());
                    return;
                }
            }

            String values = inputQueryString.substring(inputQueryString.indexOf("values") + 6, inputQueryString.length() - 1);

            ArrayList<String> valueTokenList = new ArrayList<String>(Arrays
                    .asList(values.substring(values.indexOf("(") + 1, values.indexOf(")")).split(",")));

            // fill attributes to insert
            List<CellRecords> columnToInsert = new ArrayList<>();

            for (ColumnValueConstrain colInformation : tableMetaData.columnNameAttrList) {
                int i = 0;
                boolean columnExists = false;
                for (i = 0; i < columnTokenList.size(); i++) {
                    if (columnTokenList.get(i).trim().equals(colInformation.colname)) {
                        columnExists = true;
                        try {
                            String value = valueTokenList.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokenList.get(i).trim().equals("null")) {
                                if (!colInformation.isNull) {
                                    System.out.println("Cannot Insert NULL into " + colInformation.colname);
                                    return;
                                }
                                colInformation.dataType = DBSupportedDataType.NULL;
                                value = value.toUpperCase();
                            }
                            CellRecords attr = new CellRecords(colInformation.dataType, value);
                            columnToInsert.add(attr);
                            break;
                        } catch (Exception e) {
                            System.out.println("Invalid data format for " + columnTokenList.get(i) + " with the values: "
                                    + valueTokenList.get(i));
                            return;
                        }
                    }
                }
                if (columnTokenList.size() > i) {
                    columnTokenList.remove(i);
                    valueTokenList.remove(i);
                }

                if (!columnExists) {
                    if (colInformation.isNull)
                        columnToInsert.add(new CellRecords(DBSupportedDataType.NULL, "NULL"));
                    else {
                        System.out.println("Cannot Insert NULL into " + colInformation.colname);
                        return;
                    }
                }
            }

            RandomAccessFile dsTable = new RandomAccessFile(getTableFilePath(tableName), "rw");
            int dsPageNo = BPlusTree.fetchPgNbrToInsert(dsTable, tableMetaData.rootPageNumber);
            Page dsPage = new Page(dsTable, dsPageNo);

            int rowNo = dsPage.addTableRow(tableName, columnToInsert);

            if (rowNo != -1) {

                for (int i = 0; i < tableMetaData.columnNameAttrList.size(); i++) {
                    ColumnValueConstrain col = tableMetaData.columnNameAttrList.get(i);

                    if (col.hasIdx) {
                        RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, col.colname),
                                "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.insrt(columnToInsert.get(i), rowNo);
                        indexFile.close();
                    }

                }
            }

            dsTable.close();
            if (rowNo != -1)
                System.out.println("Record Inserted Successfully.");
            System.out.println();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception while performing insert operation", e);
        }
    }
}
