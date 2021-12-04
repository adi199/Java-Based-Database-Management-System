package edu.ottawa.extensions.helpers;

import edu.ottawa.extensions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ottawa.Utils.*;

/**
 * This helper class is used to perform Delete Operation on the records
 *
 * @author Team Ottawa
 */
public class DeleteOperationHelper {

    private static final Logger logger = Logger.getLogger(DeleteOperationHelper.class.getName());

    /**
     * This method performs the delete operation
     *
     * @param deleteTableRecordQuerytring
     */
    public static void handleDeleteOperation(String deleteTableRecordQuerytring) {
        ArrayList<String> deleteTableTokens = new ArrayList<>(Arrays.asList(deleteTableRecordQuerytring.split(" ")));

        String tableName = "";

        try {

            if (!deleteTableTokens.get(1).equals("from") || !deleteTableTokens.get(2).equals("table")) {
                System.out.println("Invalid Syntax!");
                return;
            }

            tableName = deleteTableTokens.get(3);

            TableInfoHandler tableMetaData = new TableInfoHandler(tableName);
            WhereConditionProcessor whereCondition;
            try {
                whereCondition = fetchCndtFrmQuery(tableMetaData, deleteTableRecordQuerytring);

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception while performing delete operation", e);
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "rw");

            BPlusTree tree = new BPlusTree(tableFile, tableMetaData.rootPageNumber, tableMetaData.tableName);
            List<DataRecordForTable> deletedRecordList = new ArrayList<DataRecordForTable>();
            int count = 0;
            for (int pageNumber : tree.getLeafNodes(whereCondition)) {
                short deleteCountPerPage = 0;
                Page page = new Page(tableFile, pageNumber);
                for (DataRecordForTable recordDetail : page.getPageRecords()) {
                    if (whereCondition != null) {
                        if (!whereCondition.checkCondition(recordDetail.getColumnsList().get(whereCondition.columnOrdinal).fieldValue))
                            continue;
                    }

                    deletedRecordList.add(recordDetail);
                    page.DeleteTableRecord(tableName,
                            Integer.valueOf(recordDetail.idxPageHd - deleteCountPerPage).shortValue());
                    deleteCountPerPage++;
                    count++;
                }
            }

            /* Update the Index and delete all the rows if no whereCondition is given*/

            if (whereCondition == null) {
                final String tName = tableName;

                File f = new File("data/");
                File[] matchingFiles = f.listFiles((dir, name) -> name.startsWith(tName) && name.endsWith("ndx"));


                for (int i = 0; i < tableMetaData.columnNameAttrList.size(); i++) {
                    if (tableMetaData.columnNameAttrList.get(i).hasIdx) {

                        RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, tableMetaData.columnNameAttrList.get(i).colname),
                                "rw");
                        Page.addNewPage(indexFile, PageNodeType.LEAF_INDEX, -1, -1);
                        indexFile.close();

                    }
                }

            } else {
                for (int i = 0; i < tableMetaData.columnNameAttrList.size(); i++) {
                    if (tableMetaData.columnNameAttrList.get(i).hasIdx) {
                        RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, tableMetaData.columnNameAttrList.get(i).colname), "rw");
                        BTree bTree = new BTree(indexFile);
                        for (DataRecordForTable record : deletedRecordList) {
                            bTree.delete(record.getColumnsList().get(i), record.rId);
                        }
                        indexFile.close();
                    }
                }
            }

            System.out.println();
            tableFile.close();
            System.out.println(count + " Records deleted successfully.");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception occur while deleting row in table " + tableName, e);
        }

    }
}
