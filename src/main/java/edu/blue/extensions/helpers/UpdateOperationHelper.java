package edu.blue.extensions.helpers;

import edu.blue.extensions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.blue.Utils.*;

/**
 * This method helps in performing update operations of table records.
 *
 * @author Team Blue
 */
public class UpdateOperationHelper {

    private static final Logger LOGGER = Logger.getLogger(UpdateOperationHelper.class.getName());

    /**
     * This method performs the update operations
     *
     * @param updateString
     */
    public static void performUpdateOperation(String updateString) {
        ArrayList<String> updateTokens = new ArrayList<>(Arrays.asList(updateString.split(" ")));

        String table_name = updateTokens.get(1);
        List<String> columnsToUpdate = new ArrayList<>();
        List<String> valueToUpdate = new ArrayList<>();

        if (!updateTokens.get(2).equals("set") || !updateTokens.contains("=")) {
            System.out.println("Invalid Syntax!");

            return;
        }

        String updateColInfoString = updateString.split("set")[1].split("where")[0];

        List<String> column_newValueSet = Arrays.asList(updateColInfoString.split(","));

        try {
            for (String item : column_newValueSet) {
                columnsToUpdate.add(item.split("=")[0].trim());
                valueToUpdate.add(item.split("=")[1].trim().replace("\"", "").replace("'", ""));
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Update failed maybe an Invalid Syntax", e);

            return;
        }


        TableInfoHandler metadata = new TableInfoHandler(table_name);

        if (!metadata.tableExists) {
            System.out.println("Invalid Table name!");
            return;
        }

        if (!metadata.columnExists(columnsToUpdate)) {
            System.out.println("!Invalid column name(s)!");
            return;
        }

        WhereConditionProcessor condition;
        try {

            condition = getConditionFromCurrentQuery(metadata, updateString);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Update operation failed", e);
            return;

        }


        try {
            RandomAccessFile file = new RandomAccessFile(getTableFilePath(table_name), "rw");
            DBOperationsProcessor binaryFile = new DBOperationsProcessor(file);
            int noOfRecordsupdated = binaryFile.updateRecords(metadata, condition, columnsToUpdate, valueToUpdate);

            if (noOfRecordsupdated > 0) {
                List<Integer> allRowids = new ArrayList<>();
                for (ColumnValueConstrain colInfo : metadata.columnNameAttrList) {
                    for (int i = 0; i < columnsToUpdate.size(); i++)
                        if (colInfo.columnName.equals(columnsToUpdate.get(i)) && colInfo.hasIndex) {

                            // when there is no condition, All rows in the column gets updated the index value point to all rowids
                            if (condition == null) {
                                File f = new File(getIndexFilePath(table_name, colInfo.columnName));
                                if (f.exists()) {
                                    f.delete();
                                }

                                if (allRowids.size() == 0) {
                                    BPlusTree bPlusOneTree = new BPlusTree(file, metadata.rootPageNumber, metadata.tableName);
                                    for (int pageNo : bPlusOneTree.getAllLeaveNodes()) {
                                        Page currentPage = new Page(file, pageNo);
                                        for (DataRecordForTable record : currentPage.getPageRecords()) {
                                            allRowids.add(record.rowId);
                                        }
                                    }
                                }
                                //create a new index value and insert 1 index value with all rowids
                                RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(table_name, columnsToUpdate.get(i)),
                                        "rw");
                                Page.addNewPage(indexFile, PageNodeType.LEAF_INDEX, -1, -1);
                                BTree bTree = new BTree(indexFile);
                                bTree.insert(new CellRecords(colInfo.dataType, valueToUpdate.get(i)), allRowids);
                            }
                        }
                }
            }

            file.close();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception updating in table " + table_name, e);

        }
    }
}
