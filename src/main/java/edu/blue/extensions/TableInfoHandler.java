package edu.blue.extensions;

import edu.blue.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This Helper class is used to manipulate the meta data of the database tables
 * <br> davisbase_tables and davisbas_columns <br>
 * we have to update the  following meta data of the table whenever a record operations of
 * insert or delete is performed
 * 1. Record_count
 * 2. Root page Number
 *
 * @author Team Blue
 */
public class TableInfoHandler {

    private static final Logger LOGGER = Logger.getLogger(TableInfoHandler.class.getName());

    public int recordsCount;
    public List<DataRecordForTable> columnDataValues;
    public List<ColumnValueConstrain> columnNameAttrList;
    public List<String> columnNamesList;
    public String tableName;
    public boolean tableExists;
    public int rootPageNumber;

    public TableInfoHandler(String tableName) {
        this.tableName = tableName;
        tableExists = false;
        try {

            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    Utils.getTableFilePath(DBOperationsProcessor.tablesTable), "r");

            /*Obtain the root page */
            int rootPageNumber = DBOperationsProcessor.getRootPageNumber(davisbaseTablesCatalog);

            BPlusTree bplusTree = new BPlusTree(davisbaseTablesCatalog, rootPageNumber, tableName);
            /*Search all Leaf pages of the davisbase_tables*/
            for (Integer pageNumber : bplusTree.getAllLeaveNodes()) {
                Page currentPage = new Page(davisbaseTablesCatalog, pageNumber);
                /*Search through all of the records*/
                for (DataRecordForTable tableRecord : currentPage.getPageRecords()) {

                    if (tableRecord.getAttributeList().get(0).fieldValue.equals(tableName)) {
                        this.rootPageNumber = Integer.parseInt(tableRecord.getAttributeList().get(3).fieldValue);
                        recordsCount = Integer.parseInt(tableRecord.getAttributeList().get(1).fieldValue);
                        tableExists = true;
                        break;
                    }
                }
                if (tableExists)
                    break;
            }

            davisbaseTablesCatalog.close();
            if (tableExists) {
                loadColumnData();
            } else {
                throw new Exception("Table does not exist.");
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception while checking table", e.getCause());
        }
    }

    public List<Integer> getOrdinalPostion(List<String> columnsList) {
        List<Integer> ordinalPostions = new ArrayList<>();
        for (String currentColumn : columnsList) {
            ordinalPostions.add(columnNamesList.indexOf(currentColumn));
        }
        return ordinalPostions;
    }

    /**
     * This method loads the column information of the table
     */
    private void loadColumnData() {
        try {

            RandomAccessFile davisbaseColumnsFile = new RandomAccessFile(
                    Utils.getTableFilePath(DBOperationsProcessor.columnsTable), "r");
            int rootPageNumber = DBOperationsProcessor.getRootPageNumber(davisbaseColumnsFile);

            columnDataValues = new ArrayList<>();
            columnNameAttrList = new ArrayList<>();
            columnNamesList = new ArrayList<>();
            BPlusTree bPlusOneTree = new BPlusTree(davisbaseColumnsFile, rootPageNumber, tableName);

            /* Obtain all columns from the davisbase_column table and iterate all the leaf pagesto find the records with the matching table name */
            for (Integer currentPageNumber : bPlusOneTree.getAllLeaveNodes()) {

                Page page = new Page(davisbaseColumnsFile, currentPageNumber);

                for (DataRecordForTable tableRecord : page.getPageRecords()) {

                    if (tableRecord.getAttributeList().get(0).fieldValue.equals(tableName)) {
                        {
                            columnDataValues.add(tableRecord);
                            columnNamesList.add(tableRecord.getAttributeList().get(1).fieldValue);
                            ColumnValueConstrain colInfo = new ColumnValueConstrain(
                                    tableName
                                    , DBSupportedDataType.get(tableRecord.getAttributeList().get(2).fieldValue)
                                    , tableRecord.getAttributeList().get(1).fieldValue
                                    , tableRecord.getAttributeList().get(6).fieldValue.equals("YES")
                                    , tableRecord.getAttributeList().get(4).fieldValue.equals("YES")
                                    , Short.parseShort(tableRecord.getAttributeList().get(3).fieldValue)
                            );

                            if (tableRecord.getAttributeList().get(5).fieldValue.equals("PRI"))
                                colInfo.setAsPrimaryKey();

                            columnNameAttrList.add(colInfo);


                        }
                    }
                }
            }

            davisbaseColumnsFile.close();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception while getting column data from table", e);
        }

    }

    /**
     * Helper method to check if a column exists in the table
     *
     * @param columnsList
     * @return
     */
    public boolean columnExists(List<String> columnsList) {

        if (columnsList.size() == 0)
            return true;

        List<String> leftColumnsList = new ArrayList<>(columnsList);

        for (ColumnValueConstrain column_name_attr : columnNameAttrList) {
            leftColumnsList.remove(column_name_attr.columnName);
        }

        return leftColumnsList.isEmpty();
    }


    /**
     * Method to update the metedata
     */
    public void updateTableMetaData() {

        try {
            RandomAccessFile operatingTableFile = new RandomAccessFile(
                    Utils.getTableFilePath(tableName), "r");

            Integer rootPageNumber = DBOperationsProcessor.getRootPageNumber(operatingTableFile);
            operatingTableFile.close();


            RandomAccessFile davisbaseTableFile = new RandomAccessFile(
                    Utils.getTableFilePath(DBOperationsProcessor.tablesTable), "rw");

            DBOperationsProcessor tablesBinaryFile = new DBOperationsProcessor(davisbaseTableFile);

            TableInfoHandler tablesMetaData = new TableInfoHandler(DBOperationsProcessor.tablesTable);

            WhereConditionProcessor condition = new WhereConditionProcessor(DBSupportedDataType.TEXT);
            condition.setColumName("table_name");
            condition.columnOrdinal = 0;
            condition.setConditionValue(tableName);
            condition.setOperator("=");

            List<String> columnList = Arrays.asList("record_count", "root_page");
            List<String> newValues = new ArrayList<>();

            newValues.add(Integer.toString(recordsCount));
            newValues.add(Integer.toString(rootPageNumber));

            tablesBinaryFile.updateRecords(tablesMetaData, condition, columnList, newValues);

            davisbaseTableFile.close();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception updating the metadata for the table", e);
        }


    }

    /**
     * Insertion operation is validated for unique constraint and if it can be null
     *
     * @param rowAttributesList
     * @return
     * @throws IOException
     */
    public boolean validateInsert(List<CellRecords> rowAttributesList) throws IOException {
        RandomAccessFile operatingTableFile = new RandomAccessFile(Utils.getTableFilePath(tableName), "r");
        DBOperationsProcessor file = new DBOperationsProcessor(operatingTableFile);


        for (int i = 0; i < columnNameAttrList.size(); i++) {

            WhereConditionProcessor condition = new WhereConditionProcessor(columnNameAttrList.get(i).dataType);
            condition.columnName = columnNameAttrList.get(i).columnName;
            condition.columnOrdinal = i;
            condition.setOperator("=");

            if (columnNameAttrList.get(i).isUnique) {
                condition.setConditionValue(rowAttributesList.get(i).fieldValue);
                if (file.checkIfRecordExists(this, condition)) {
                    System.out.println("! Insert failed: Column " + columnNameAttrList.get(i).columnName + " should be unique.");
                    operatingTableFile.close();
                    return false;
                }
            }
        }
        operatingTableFile.close();
        return true;
    }

}