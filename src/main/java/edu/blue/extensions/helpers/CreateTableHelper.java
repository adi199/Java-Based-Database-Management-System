package edu.blue.extensions.helpers;

import edu.blue.extensions.*;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.blue.Utils.getTableFilePath;
import static edu.blue.extensions.helpers.DeleteOperationHelper.handleDeleteOperation;
import static edu.blue.extensions.helpers.IndexCreationHelper.handleIndexCreation;

/**
 * This helper class is used to create table in the Database
 *
 * @author Team Ottawa
 */
public class CreateTableHelper {

    // logs event
    private static final Logger logger = Logger.getLogger(CreateTableHelper.class.getName());

    /**
     * This method perform the table creation operation
     *
     * @param createTableQueryString
     */
    //
    public static void handleCreateTableOperation(String createTableQueryString) {
        ArrayList<String> createTableTokens = new ArrayList<>(Arrays.asList(createTableQueryString.split(" ")));
        if (!createTableTokens.get(1).equals("table") || (!createTableQueryString.contains("(") || !createTableQueryString.contains(")"))) {
            logger.warning("Invalid Syntax!");

            return;
        }
        String tableName = createTableTokens.get(2);
        if (tableName.trim().length() == 0) {
            logger.warning("Please provide name of Table.");
            return;
        }
        try {

            if (tableName.contains("(")) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }

            List<ColumnValueConstrain> columnList = new ArrayList<>();
            ArrayList<String> columnTokens = new ArrayList<>(Arrays.asList(createTableQueryString
                    .substring(createTableQueryString.indexOf("(") + 1, createTableQueryString.length() - 1).split(",")));

            short order = 1;

            String primaryKeyCol = "";

            // Loop through column tokens
            for (String columnToken : columnTokens) {

                ArrayList<String> columnInfoToken = new ArrayList<>(Arrays.asList(columnToken.trim().split(" ")));
                ColumnValueConstrain colInformation = new ColumnValueConstrain();
                colInformation.tableName = tableName;
                colInformation.columnName = columnInfoToken.get(0);
                colInformation.isNullable = true;
                colInformation.dataType = DBSupportedDataType.get(columnInfoToken.get(1).toUpperCase());
                for (int i = 0; i < columnInfoToken.size(); i++) {

                    if ((columnInfoToken.get(i).equals("null"))) {
                        colInformation.isNullable = true;
                    }
                    if (columnInfoToken.get(i).contains("not") && (columnInfoToken.get(i + 1).contains("null"))) {
                        colInformation.isNullable = false;
                        i++;
                    }

                    if ((columnInfoToken.get(i).equals("unique"))) {
                        colInformation.isUnique = true;
                    } else if (columnInfoToken.get(i).contains("primary") && (columnInfoToken.get(i + 1).contains("key"))) {
                        colInformation.isPrimaryKey = true;
                        colInformation.isUnique = true;
                        colInformation.isNullable = false;
                        primaryKeyCol = colInformation.columnName;
                        i++;
                    }

                }
                colInformation.ordinalPosition = order++;
                columnList.add(colInformation);

            }

            // Updates system files
            RandomAccessFile tablesCatalog = new RandomAccessFile(
                    getTableFilePath(DBOperationsProcessor.tablesTable), "rw");
            TableInfoHandler tableMetaData = new TableInfoHandler(DBOperationsProcessor.tablesTable);

            int pageNumber = BPlusTree.getPageNumberForInsertion(tablesCatalog, tableMetaData.rootPageNumber);

            Page pageDetail = new Page(tablesCatalog, pageNumber);

            int rowNo = pageDetail.addTableRow(DBOperationsProcessor.tablesTable,
                    Arrays.asList(new CellRecords[]{new CellRecords(DBSupportedDataType.TEXT, tableName), // DBOperationsProcessor.tablesTable->test
                            new CellRecords(DBSupportedDataType.INT, "0"), new CellRecords(DBSupportedDataType.SMALLINT, "0"),
                            new CellRecords(DBSupportedDataType.SMALLINT, "0")}));
            tablesCatalog.close();

            if (rowNo == -1) {
                logger.warning("Duplicate table Name, Please provide unique table name.");
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "rw");
            Page.addNewPage(tableFile, PageNodeType.LEAF_TYPE, -1, -1);
            tableFile.close();

            RandomAccessFile tableColumnsCatalog = new RandomAccessFile(
                    getTableFilePath(DBOperationsProcessor.columnsTable), "rw");
            TableInfoHandler tableColumnsMetaData = new TableInfoHandler(DBOperationsProcessor.columnsTable);
            pageNumber = BPlusTree.getPageNumberForInsertion(tableColumnsCatalog, tableColumnsMetaData.rootPageNumber);

            Page page1 = new Page(tableColumnsCatalog, pageNumber);

            for (ColumnValueConstrain columnDetail : columnList) {
                page1.addNewColumn(columnDetail);
            }

            tableColumnsCatalog.close();

            System.out.println("Table created successfully.");

            if (primaryKeyCol.length() > 0) {
                handleIndexCreation("create index on " + tableName + "(" + primaryKeyCol + ")");
            }
        } catch (Exception e) {

            logger.log(Level.SEVERE, "Exception while creating table, might be Syntax Error!", e);
            handleDeleteOperation("delete from table " + DBOperationsProcessor.tablesTable + " where table_name = '" + tableName
                    + "' ");
            handleDeleteOperation("delete from table " + DBOperationsProcessor.columnsTable + " where table_name = '" + tableName
                    + "' ");
        }

    }
}
