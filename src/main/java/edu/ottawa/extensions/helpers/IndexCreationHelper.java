package edu.ottawa.extensions.helpers;

import edu.ottawa.Utils;
import edu.ottawa.extensions.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ottawa.Utils.getIndexFilePath;
import static edu.ottawa.Utils.getTableFilePath;

/**
 * This helper is used create a index on one of the tables columns.
 *
 * @author Team Ottawa
 */
public class IndexCreationHelper {

    private static final Logger logger = Logger.getLogger(IndexCreationHelper.class.getName());

    /**
     * This method to create index
     *
     * @param createIndexString
     */
    public static void handleIndexCreation(String createIndexString) {
        ArrayList<String> createIndexTokens = new ArrayList<>(Arrays.asList(createIndexString.split(" ")));
        try {
            if (!createIndexTokens.get(2).equals("on") || !createIndexString.contains("(")
                    || !createIndexString.contains(")") && createIndexTokens.size() < 4) {
                System.out.println("Invalid Syntax!");
                return;
            }

            String tableName = createIndexString
                    .substring(createIndexString.indexOf("on") + 3, createIndexString.indexOf("(")).trim();
            String columnName = createIndexString
                    .substring(createIndexString.indexOf("(") + 1, createIndexString.indexOf(")")).trim();

            // Checks if the required column Index already exists
            if (new File(Utils.getIndexFilePath(tableName, columnName)).exists()) {
                System.out.println("Column Index already exists");
                return;
            }

            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "rw");

            TableInfoHandler tableMetaData = new TableInfoHandler(tableName);

            if (!tableMetaData.tableExists) {
                System.out.println("Invalid Table");
                tableFile.close();
                return;
            }

            int columnOrdinal = tableMetaData.columnNamesList.indexOf(columnName);

            if (columnOrdinal < 0) {
                System.out.println("Invalid column name(s)");
                tableFile.close();
                return;
            }


            // Creates a Index file
            RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, columnName), "rw");
            Page.addNewPage(indexFile, PageNodeType.LEAF_INDEX, -1, -1);


            if (tableMetaData.recordsCount > 0) {
                BPlusTree bPlusOneTree = new BPlusTree(tableFile, tableMetaData.rootPageNumber, tableMetaData.tableName);
                for (int pageNo : bPlusOneTree.getLeafNodes()) {
                    Page page = new Page(tableFile, pageNo);
                    BTree bTree = new BTree(indexFile);
                    for (DataRecordForTable record : page.getPageRecords()) {
                        bTree.insrt(record.getColumnsList().get(columnOrdinal), record.rId);
                    }
                }
            }

            System.out.println("Index successfully created on the column : " + columnName);
            indexFile.close();
            tableFile.close();

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception while creating Index");
        }

    }
}
