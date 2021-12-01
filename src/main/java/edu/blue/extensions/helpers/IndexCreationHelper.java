package edu.blue.extensions.helpers;

import edu.blue.Utils;
import edu.blue.extensions.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.blue.Utils.getIndexFilePath;
import static edu.blue.Utils.getTableFilePath;

/**
 * This helper class is invoked to create a index on one of the tables columns.
 *
 * @author Team Blue
 */
public class IndexCreationHelper {

    private static final Logger LOGGER = Logger.getLogger(IndexCreationHelper.class.getName());

    /**
     * This method performs the creation of index operation
     *
     * @param createIndexString
     */
    public static void performIndexCreation(String createIndexString) {
        ArrayList<String> createIndexTokens = new ArrayList<>(Arrays.asList(createIndexString.split(" ")));
        try {
            if (!createIndexTokens.get(2).equals("on") || !createIndexString.contains("(")
                    || !createIndexString.contains(")") && createIndexTokens.size() < 4) {
                System.out.println("Error! Invalid Syntax!");
                return;
            }

            String tableName = createIndexString
                    .substring(createIndexString.indexOf("on") + 3, createIndexString.indexOf("(")).trim();
            String columnName = createIndexString
                    .substring(createIndexString.indexOf("(") + 1, createIndexString.indexOf(")")).trim();

            /* Checks if the required column Index already exists */
            if (new File(Utils.getIndexFilePath(tableName, columnName)).exists()) {
                System.out.println("Index already exists!");
                return;
            }

            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "rw");

            TableInfoHandler metaData = new TableInfoHandler(tableName);

            if (!metaData.tableExists) {
                System.out.println("Invalid Table name!");
                tableFile.close();
                return;
            }

            int columnOrdinal = metaData.columnNamesList.indexOf(columnName);

            if (columnOrdinal < 0) {
                System.out.println("Invalid column name(s)!");
                tableFile.close();
                return;
            }


            /* Creates a Index file*/
            RandomAccessFile indexFile = new RandomAccessFile(getIndexFilePath(tableName, columnName), "rw");
            Page.addNewPage(indexFile, PageNodeType.LEAF_INDEX, -1, -1);


            if (metaData.recordsCount > 0) {
                BPlusTree bPlusOneTree = new BPlusTree(tableFile, metaData.rootPageNumber, metaData.tableName);
                for (int pageNo : bPlusOneTree.getAllLeaveNodes()) {
                    Page page = new Page(tableFile, pageNo);
                    BTree bTree = new BTree(indexFile);
                    for (DataRecordForTable record : page.getPageRecords()) {
                        bTree.insert(record.getAttributeList().get(columnOrdinal), record.rowId);
                    }
                }
            }

            System.out.println("Index successfully created on the column : " + columnName);
            indexFile.close();
            tableFile.close();

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception in creating Index");
        }

    }
}
