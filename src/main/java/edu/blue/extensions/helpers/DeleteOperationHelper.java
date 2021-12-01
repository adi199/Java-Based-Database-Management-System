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
 * This helper class is used for performing Delete Operation on the records
 *
 * @author Team Blue
 */
public class DeleteOperationHelper {

    private static final Logger LOGGER = Logger.getLogger(DeleteOperationHelper.class.getName());

    /**
     * This method performs the delete operation
     *
     * @param deleteTableString
     */
    public static void performDeleteOperation(String deleteTableString) {
        ArrayList<String> deleteTableTokens = new ArrayList<>(Arrays.asList(deleteTableString.split(" ")));

        String tableName = "";

        try {

            if (!deleteTableTokens.get(1).equals("from") || !deleteTableTokens.get(2).equals("table")) {
                System.out.println("Please check your Syntax!");
                return;
            }

            tableName = deleteTableTokens.get(3);

            TableInfoHandler metaData = new TableInfoHandler(tableName);
            WhereConditionProcessor condition;
            try {
                condition = getConditionFromCurrentQuery(metaData, deleteTableString);

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Exception while deleting", e);
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "rw");

            BPlusTree tree = new BPlusTree(tableFile, metaData.rootPageNumber, metaData.tableName);
            List<DataRecordForTable> deletedRecords = new ArrayList<DataRecordForTable>();
            int count = 0;
            for (int pageNo : tree.getAllLeaveNodes(condition)) {
                short deleteCountPerPage = 0;
                Page page = new Page(tableFile, pageNo);
                for (DataRecordForTable record : page.getPageRecords()) {
                    if (condition != null) {
                        if (!condition.checkCondition(record.getAttributeList().get(condition.columnOrdinal).fieldValue))
                            continue;
                    }

                    deletedRecords.add(record);
                    page.DeleteTableRecord(tableName,
                            Integer.valueOf(record.IndexPageHead - deleteCountPerPage).shortValue());
                    deleteCountPerPage++;
                    count++;
                }
            }

            /* Update the Index and delete all the rows if no condition is given*/

            if (condition == null) {
                final String table_Name = tableName;

                File f = new File("data/");
                File[] matchingFiles = f.listFiles((dir, name) -> name.startsWith(table_Name) && name.endsWith("ndx"));


                for (int i = 0; i < metaData.columnNameAttrList.size(); i++) {
                    if (metaData.columnNameAttrList.get(i).hasIndex) {

                        RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, metaData.columnNameAttrList.get(i).columnName),
                                "rw");
                        Page.addNewPage(indexFile, PageNodeType.LEAF_INDEX, -1, -1);
                        indexFile.close();

                    }
                }

            } else {
                for (int i = 0; i < metaData.columnNameAttrList.size(); i++) {
                    if (metaData.columnNameAttrList.get(i).hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, metaData.columnNameAttrList.get(i).columnName), "rw");
                        BTree bTree = new BTree(indexFile);
                        for (DataRecordForTable record : deletedRecords) {
                            bTree.delete(record.getAttributeList().get(i), record.rowId);
                        }
                        indexFile.close();
                    }
                }
            }

            System.out.println();
            tableFile.close();
            System.out.println(count + " records deleted successfully.");

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception while deleting row in table " + tableName, e);
        }

    }
}
