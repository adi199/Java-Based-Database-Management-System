package edu.blue.extensions.helpers;

import edu.blue.extensions.*;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.blue.Utils.getIndexFilePath;
import static edu.blue.Utils.getTableFilePath;

/**
 * This helper class is used to perform insert operation on the tables
 *
 * @author Team Blue
 */
public class InsertOperationHelper {

    private static final Logger LOGGER = Logger.getLogger(InsertOperationHelper.class.getName());

    /**
     * This method performs the insertion operation
     *
     * @param queryString
     */
    public static void performInsertOperation(String queryString) {
        ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));

        if (!insertTokens.get(1).equals("into") || !queryString.contains(") values")) {
            System.out.println("Invalid Syntax!");
            return;
        }

        try {
            String tableName = insertTokens.get(2);
            if (tableName.trim().length() == 0) {
                System.out.println("Table name cannot be empty!");
                return;
            }

            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }
            TableInfoHandler dstMetaData = new TableInfoHandler(tableName);

            if (!dstMetaData.tableExists) {
                System.out.println("Table does not exist!");
                return;
            }

            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(
                    queryString.substring(queryString.indexOf("(") + 1, queryString.indexOf(") values")).split(",")));

            // Column List validation
            for (String colToken : columnTokens) {
                if (!dstMetaData.columnNamesList.contains(colToken.trim())) {
                    System.out.println("Invalid column : " + colToken.trim());
                    return;
                }
            }

            String valuesString = queryString.substring(queryString.indexOf("values") + 6, queryString.length() - 1);

            ArrayList<String> valueTokens = new ArrayList<String>(Arrays
                    .asList(valuesString.substring(valuesString.indexOf("(") + 1, valuesString.indexOf(")")).split(",")));

            // fill attributes to insert
            List<CellRecords> attributeToInsert = new ArrayList<>();

            for (ColumnValueConstrain colInfo : dstMetaData.columnNameAttrList) {
                int i = 0;
                boolean columnProvided = false;
                for (i = 0; i < columnTokens.size(); i++) {
                    if (columnTokens.get(i).trim().equals(colInfo.columnName)) {
                        columnProvided = true;
                        try {
                            String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokens.get(i).trim().equals("null")) {
                                if (!colInfo.isNullable) {
                                    System.out.println("Cannot Insert NULL into " + colInfo.columnName);
                                    return;
                                }
                                colInfo.dataType = DBSupportedDataType.NULL;
                                value = value.toUpperCase();
                            }
                            CellRecords attr = new CellRecords(colInfo.dataType, value);
                            attributeToInsert.add(attr);
                            break;
                        } catch (Exception e) {
                            System.out.println("Invalid data format for " + columnTokens.get(i) + " values: "
                                    + valueTokens.get(i));
                            return;
                        }
                    }
                }
                if (columnTokens.size() > i) {
                    columnTokens.remove(i);
                    valueTokens.remove(i);
                }

                if (!columnProvided) {
                    if (colInfo.isNullable)
                        attributeToInsert.add(new CellRecords(DBSupportedDataType.NULL, "NULL"));
                    else {
                        System.out.println("Cannot Insert NULL into " + colInfo.columnName);
                        return;
                    }
                }
            }

            RandomAccessFile dstTable = new RandomAccessFile(getTableFilePath(tableName), "rw");
            int dstPageNo = BPlusTree.getPageNumberForInsertion(dstTable, dstMetaData.rootPageNumber);
            Page dstPage = new Page(dstTable, dstPageNo);

            int rowNo = dstPage.addTableRow(tableName, attributeToInsert);

            if (rowNo != -1) {

                for (int i = 0; i < dstMetaData.columnNameAttrList.size(); i++) {
                    ColumnValueConstrain col = dstMetaData.columnNameAttrList.get(i);

                    if (col.hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, col.columnName),
                                "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.insert(attributeToInsert.get(i), rowNo);
                        indexFile.close();
                    }

                }
            }

            dstTable.close();
            if (rowNo != -1)
                System.out.println("Record Inserted Successfully.");
            System.out.println();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception performing insert operation", e);
        }
    }
}
