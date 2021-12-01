package edu.blue.extensions.helpers;

import edu.blue.extensions.*;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.blue.Utils.getTableFilePath;
import static edu.blue.extensions.helpers.DeleteOperationHelper.performDeleteOperation;
import static edu.blue.extensions.helpers.IndexCreationHelper.performIndexCreation;

/**
 * This helper class is used for Table creation in the Database
 *
 * @author Team Blue
 */
public class CreateTableHelper {

    private static final Logger LOGGER = Logger.getLogger(CreateTableHelper.class.getName());

    /**
     * This method perform the table creation operation
     *
     * @param createTableString
     */
    public static void performCreateTableOperation(String createTableString) {
        ArrayList<String> createTableTokens = new ArrayList<>(Arrays.asList(createTableString.split(" ")));
        if (!createTableTokens.get(1).equals("table") || (!createTableString.contains("(") || !createTableString.contains(")"))) {
            LOGGER.warning("Please check Syntax!");

            return;
        }
        String tableName = createTableTokens.get(2);
        if (tableName.trim().length() == 0) {
            LOGGER.warning("Table name is required!");
            return;
        }
        try {

            if (tableName.contains("(")) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }

            List<ColumnValueConstrain> lstcolumnInformation = new ArrayList<>();
            ArrayList<String> columnTokens = new ArrayList<>(Arrays.asList(createTableString
                    .substring(createTableString.indexOf("(") + 1, createTableString.length() - 1).split(",")));

            short ordinalPosition = 1;

            String primaryKeyColumn = "";

            for (String columnToken : columnTokens) {

                ArrayList<String> colInfoToken = new ArrayList<>(Arrays.asList(columnToken.trim().split(" ")));
                ColumnValueConstrain colInfo = new ColumnValueConstrain();
                colInfo.tableName = tableName;
                colInfo.columnName = colInfoToken.get(0);
                colInfo.isNullable = true;
                colInfo.dataType = DBSupportedDataType.get(colInfoToken.get(1).toUpperCase());
                for (int i = 0; i < colInfoToken.size(); i++) {

                    if ((colInfoToken.get(i).equals("null"))) {
                        colInfo.isNullable = true;
                    }
                    if (colInfoToken.get(i).contains("not") && (colInfoToken.get(i + 1).contains("null"))) {
                        colInfo.isNullable = false;
                        i++;
                    }

                    if ((colInfoToken.get(i).equals("unique"))) {
                        colInfo.isUnique = true;
                    } else if (colInfoToken.get(i).contains("primary") && (colInfoToken.get(i + 1).contains("key"))) {
                        colInfo.isPrimaryKey = true;
                        colInfo.isUnique = true;
                        colInfo.isNullable = false;
                        primaryKeyColumn = colInfo.columnName;
                        i++;
                    }

                }
                colInfo.ordinalPosition = ordinalPosition++;
                lstcolumnInformation.add(colInfo);

            }

            /* Updates system files */
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    getTableFilePath(DBOperationsProcessor.tablesTable), "rw");
            TableInfoHandler davisbaseTableMetaData = new TableInfoHandler(DBOperationsProcessor.tablesTable);

            int pageNo = BPlusTree.getPageNumberForInsertion(davisbaseTablesCatalog, davisbaseTableMetaData.rootPageNumber);

            Page page = new Page(davisbaseTablesCatalog, pageNo);

            int rowNo = page.addTableRow(DBOperationsProcessor.tablesTable,
                    Arrays.asList(new CellRecords[]{new CellRecords(DBSupportedDataType.TEXT, tableName), // DBOperationsProcessor.tablesTable->test
                            new CellRecords(DBSupportedDataType.INT, "0"), new CellRecords(DBSupportedDataType.SMALLINT, "0"),
                            new CellRecords(DBSupportedDataType.SMALLINT, "0")}));
            davisbaseTablesCatalog.close();

            if (rowNo == -1) {
                LOGGER.warning("! Duplicate table Name");
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(getTableFilePath(tableName), "rw");
            Page.addNewPage(tableFile, PageNodeType.LEAF_TYPE, -1, -1);
            tableFile.close();

            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
                    getTableFilePath(DBOperationsProcessor.columnsTable), "rw");
            TableInfoHandler davisbaseColumnsMetaData = new TableInfoHandler(DBOperationsProcessor.columnsTable);
            pageNo = BPlusTree.getPageNumberForInsertion(davisbaseColumnsCatalog, davisbaseColumnsMetaData.rootPageNumber);

            Page page1 = new Page(davisbaseColumnsCatalog, pageNo);

            for (ColumnValueConstrain column : lstcolumnInformation) {
                page1.addNewColumn(column);
            }

            davisbaseColumnsCatalog.close();

            System.out.println("Table created successfully.");

            if (primaryKeyColumn.length() > 0) {
                performIndexCreation("create index on " + tableName + "(" + primaryKeyColumn + ")");
            }
        } catch (Exception e) {

            LOGGER.log(Level.SEVERE, "Exception creating table, might be Syntax Error!", e);
            performDeleteOperation("delete from table " + DBOperationsProcessor.tablesTable + " where table_name = '" + tableName
                    + "' ");
            performDeleteOperation("delete from table " + DBOperationsProcessor.columnsTable + " where table_name = '" + tableName
                    + "' ");
        }

    }
}
