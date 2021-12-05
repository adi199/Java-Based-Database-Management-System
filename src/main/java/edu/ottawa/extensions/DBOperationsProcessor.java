package edu.ottawa.extensions;

import edu.ottawa.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


//The operations processor is helpful during performing operations on tables
public class DBOperationsProcessor {

    private static final Logger LOGGER = Logger.getLogger(DBOperationsProcessor.class.getName());

    public static String columnsTable = "davisbase_columns";
    public static String tablesTable = "davisbase_tables";
    public static boolean showRowId = false;
    public static boolean dataStoreInitialized = false;

    static int page_size_degree = 9;
    static int pageSize = (int) Math.pow(2, page_size_degree);

    RandomAccessFile file;

    public DBOperationsProcessor(RandomAccessFile file) {
        this.file = file;
    }

    //This function checks whether a record exists or not
    public boolean checkIfRecordExists(TableInfoHandler tableMetaData, WhereConditionProcessor condition) throws IOException {

        BPlusTree bPlusTree = new BPlusTree(file, tableMetaData.rootPageNumber, tableMetaData.tableName);


        for (Integer pageNo : bPlusTree.getLeafNodes(condition)) {
            Page newpage = new Page(file, pageNo);
            for (DataRecordForTable record : newpage.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(record.getColumnsList().get(condition.columnOrdinal).fieldValue))
                        continue;
                }
                return true;
            }
        }
        return false;

    }

    //this function will select a record from a specified table
    public void selectRecordsFromTable(TableInfoHandler tableMetaData, List<String> columNames, WhereConditionProcessor condition) throws IOException {

        //According to the table positions, the order might be different.
        List<Integer> indexPositionList = tableMetaData.getOrdinalPosition(columNames);

        System.out.println();

        List<Integer> printingPositionList = new ArrayList<>();

        int columnOutputLength = 0;
        printingPositionList.add(columnOutputLength);
        int completeTablePrintLength = 0;
        if (showRowId) {
            System.out.print("rowid");
            System.out.print(Utils.printSeparator(" ", 5));
            printingPositionList.add(10);
            completeTablePrintLength += 10;
        }


        for (int i : indexPositionList) {
            String columnName = tableMetaData.columnNameAttrList.get(i).colname;
            columnOutputLength = Math.max(columnName.length()
                    , tableMetaData.columnNameAttrList.get(i).dataType.getPrintOffset()) + 5;
            printingPositionList.add(columnOutputLength);
            System.out.print(columnName);
            System.out.print(Utils.printSeparator(" ", columnOutputLength - columnName.length()));
            completeTablePrintLength += columnOutputLength;
        }
        System.out.println();
        System.out.println(Utils.printSeparator("-", completeTablePrintLength));

        BPlusTree bPlusTree = new BPlusTree(file, tableMetaData.rootPageNumber, tableMetaData.tableName);

        String currValue;
        int count = 0;
        for (Integer workingPageNumber : bPlusTree.getLeafNodes(condition)) {
            Page page = new Page(file, workingPageNumber);
            for (DataRecordForTable tableRecord : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(tableRecord.getColumnsList().get(condition.columnOrdinal).fieldValue))
                        continue;
                }
                int columnCount = 0;
                if (showRowId) {
                    currValue = Integer.valueOf(tableRecord.rId).toString();
                    System.out.print(currValue);
                    System.out.print(Utils.printSeparator(" ", printingPositionList.get(++columnCount) - currValue.length()));
                }
                for (int w : indexPositionList) {
                    currValue = tableRecord.getColumnsList().get(w).fieldValue;
                    System.out.print(currValue);
                    System.out.print(Utils.printSeparator(" ", printingPositionList.get(++columnCount) - currValue.length()));
                }
                System.out.println();
                count++;
            }
        }
        System.out.println();
        System.out.println(count + " record(s) retrieved successfully.");
    }

    //This function updates existing record in the table
    public int updateRecords(TableInfoHandler tableMetaData, WhereConditionProcessor condition,
                             List<String> columNames, List<String> newValues) throws IOException {
        int count = 0;


        List<Integer> indexedPositions = tableMetaData.getOrdinalPosition(columNames);

        int k = 0;
        Map<Integer, CellRecords> newValueMap = new HashMap<>();

        for (String strnewValue : newValues) {
            int index = indexedPositions.get(k);

            try {
                newValueMap.put(index,
                        new CellRecords(tableMetaData.columnNameAttrList.get(index).dataType, strnewValue));
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Invalid data format", e);
                return count;
            }

            k++;
        }

        BPlusTree bPlusOneTree = new BPlusTree(file, tableMetaData.rootPageNumber, tableMetaData.tableName);

        List<Integer> updateRowids = new ArrayList<>();
        int optionalIndex = -1;
        for (Integer pageNo : bPlusOneTree.getLeafNodes(condition)) {
            short deleteCountPerPage = 0;
            Page page = new Page(file, pageNo);
            for (DataRecordForTable record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(record.getColumnsList().get(condition.columnOrdinal).fieldValue))
                        continue;
                }
                if (!updateRowids.contains(record.rId))
                    count++;

                List<CellRecords> attributeList = record.getColumnsList();
                for (int i : newValueMap.keySet()) {
                    CellRecords oldValue = record.getColumnsList().get(i);
                    int rowId = record.rId;
                    if ((record.getColumnsList().get(i).dataType == DBSupportedDataType.TEXT
                            && record.getColumnsList().get(i).fieldValue.length() == newValueMap.get(i).fieldValue.length())
                            || (record.getColumnsList().get(i).dataType != DBSupportedDataType.NULL && record.getColumnsList().get(i).dataType != DBSupportedDataType.TEXT)
                    ) {
                        page.updateRecord(record, i, newValueMap.get(i).fieldValueByte);
                        CellRecords attr = attributeList.get(i);
                        attributeList.remove(i);
                        attr = newValueMap.get(i);
                        attributeList.add(i, attr);
                    } else {
                        //Delete the record ,insert a new update index;
                        if (condition != null)
                            optionalIndex = condition.columnOrdinal;

                        page.DeleteTableRecord(tableMetaData.tableName,
                                Integer.valueOf(record.idxPageHd - deleteCountPerPage).shortValue());
                        deleteCountPerPage++;

                        CellRecords attr = attributeList.get(i);
                        attributeList.remove(i);
                        attr = newValueMap.get(i);
                        attributeList.add(i, attr);
                        int pageNumberForInsertion = BPlusTree.fetchPgNbrToInsert(file, tableMetaData.rootPageNumber);
                        Page pageToInsert = new Page(file, pageNumberForInsertion);
                        rowId = pageToInsert.addTableRow(tableMetaData.tableName, attributeList);
                        updateRowids.add(rowId);
                    }

                    if (tableMetaData.columnNameAttrList.get(i).hasIdx && condition != null
                    ) {
                        RandomAccessFile indexFile = new RandomAccessFile(Utils.getIndexFilePath(tableMetaData.columnNameAttrList.get(i).tblName, tableMetaData.columnNameAttrList.get(i).colname), "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.delete(oldValue, record.rId);
                        bTree.insrt(newValueMap.get(i), rowId);
                        indexFile.close();
                    }
                    if (optionalIndex != -1 && tableMetaData.columnNameAttrList.get(optionalIndex).hasIdx) {
                        RandomAccessFile indexFile = new RandomAccessFile(Utils.getIndexFilePath(tableMetaData.columnNameAttrList.get(optionalIndex).tblName, tableMetaData.columnNameAttrList.get(optionalIndex).colname), "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.delete(record.getColumnsList().get(optionalIndex), record.rId);
                        bTree.insrt(record.getColumnsList().get(optionalIndex), rowId);
                        indexFile.close();
                    }

                }
            }
        }

        if (!tableMetaData.tableName.equals(tablesTable) && !tableMetaData.tableName.equals(columnsTable))
            System.out.println(count + " record(s) updated successfully.");

        return count;

    }

    //this will find the root page number
    public static int getRootPageNumber(RandomAccessFile binaryfile) {
        int rootpageValue = 0;
        try {
            for (int i = 0; i < binaryfile.length() / DBOperationsProcessor.pageSize; i++) {
                binaryfile.seek(i * DBOperationsProcessor.pageSize + 0x0A);
                int readnumber = binaryfile.readInt();

                if (readnumber == -1) {
                    return i;
                }
            }
            return rootpageValue;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception creating the root page number", e);
        }
        return -1;

    }

    /**
     * The DavisBase data storage container is created by this static method which then
     * initializes two .tbl files to implement the two system tables,
     * davisbase_tables and davisbase_columns. Calling this method will reset the DB
     */
    public static void initializeDataBaseStore() {

        File userDataStorageDirectory = new File("data/user_data");
        try {
            File dbDataStorageDirectory = new File("data");
            dbDataStorageDirectory.mkdir();


            String[] staleFiles;
            staleFiles = dbDataStorageDirectory.list();
            assert staleFiles != null;
            for (String currentStaleFile : staleFiles) {
                File anOldFile = new File(dbDataStorageDirectory, currentStaleFile);
                anOldFile.delete();
            }
        } catch (SecurityException se) {
            LOGGER.log(Level.SEVERE, "Security Exception,Unable to create data directory for DB", se);

        }

        try {
            int currentOperatingPageNumber = 0;

            RandomAccessFile dbTablesCatalog = new RandomAccessFile(
                    Utils.getTableFilePath(tablesTable), "rw");
            Page.addNewPage(dbTablesCatalog, PageNodeType.LEAF_TYPE, -1, -1);
            Page page = new Page(dbTablesCatalog, currentOperatingPageNumber);


            page.addTableRow(tablesTable, Arrays.asList(new CellRecords[]{
                    new CellRecords(DBSupportedDataType.TEXT, DBOperationsProcessor.columnsTable),
                    new CellRecords(DBSupportedDataType.INT, "11"),
                    new CellRecords(DBSupportedDataType.SMALLINT, "0"),
                    new CellRecords(DBSupportedDataType.SMALLINT, "2")}));

            page.addTableRow(tablesTable, Arrays.asList(new CellRecords[]{
                    new CellRecords(DBSupportedDataType.TEXT, DBOperationsProcessor.tablesTable),
                    new CellRecords(DBSupportedDataType.INT, "2"),
                    new CellRecords(DBSupportedDataType.SMALLINT, "0"),
                    new CellRecords(DBSupportedDataType.SMALLINT, "0")
            }));

            dbTablesCatalog.close();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed creating dtabase_tables file", e);
        }

        //Here we create davis base column catalog
        try {
            RandomAccessFile dbColumnFile = new RandomAccessFile(
                    Utils.getTableFilePath(columnsTable), "rw");
            Page.addNewPage(dbColumnFile, PageNodeType.LEAF_TYPE, -1, -1);
            Page page = new Page(dbColumnFile, 0);

            short indexPosition = 1;

            /*Add new values into tables*/
            page.addNewColumn(new ColumnValueConstrain(tablesTable, DBSupportedDataType.TEXT, "table_name", true, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(tablesTable, DBSupportedDataType.INT, "record_count", false, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(tablesTable, DBSupportedDataType.SMALLINT, "avg_length", false, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(tablesTable, DBSupportedDataType.SMALLINT, "root_page", false, false, indexPosition++));


            indexPosition = 1;
            /* Add values to columns table*/
            page.addNewColumn(new ColumnValueConstrain(columnsTable, DBSupportedDataType.TEXT, "table_name", false, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(columnsTable, DBSupportedDataType.TEXT, "column_name", false, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(columnsTable, DBSupportedDataType.SMALLINT, "data_type", false, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(columnsTable, DBSupportedDataType.SMALLINT, "ordinal_position", false, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(columnsTable, DBSupportedDataType.TEXT, "is_nullable", false, false, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(columnsTable, DBSupportedDataType.SMALLINT, "column_key", false, true, indexPosition++));
            page.addNewColumn(new ColumnValueConstrain(columnsTable, DBSupportedDataType.SMALLINT, "is_unique", false, false, indexPosition++));

            dbColumnFile.close();
            userDataStorageDirectory.mkdir();
            dataStoreInitialized = true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed creating database_columns file", e);
        }
    }
}


