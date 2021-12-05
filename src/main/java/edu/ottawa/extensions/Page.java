package edu.ottawa.extensions;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all operations related to page
 *
 * @author Team Ottawa
 */
public class Page {

    private static final Logger LOGGER = Logger.getLogger(Page.class.getName());

    public DBSupportedDataType inType;
    public TreeSet<Long> lIndexValues;
    public TreeSet<String> sIndexValues;
    public HashMap<String, IndexRecord> pointer;
    private Map<Integer, DataRecordForTable> map;
    public PageNodeType pageType;
    short noOfCells = 0;
    public int pageNumber;
    short startOffSet;
    public int rPage;
    public int parPageNum;
    private List<DataRecordForTable> records;
    boolean isRefreshed = false;
    long start;
    int lastId;
    int remainSpace;
    RandomAccessFile binaryFile;
    List<InternalTableRecord> lChild;
    private IndexNode insertNode;
    private boolean idxPageCleaned;


    /**
     * Reads the page header from the page and fills the attribute values by reading from file
     *
     * @param file
     * @param pageNumber
     */
    public Page(RandomAccessFile file, int pageNumber) {
        try {
            this.pageNumber = pageNumber;
            inType = null;
            lIndexValues = new TreeSet<>();
            sIndexValues = new TreeSet<>();
            pointer = new HashMap<>();
            map = new HashMap<>();

            this.binaryFile = file;
            lastId = 0;
            start = (long) DBOperationsProcessor.pageSize * pageNumber;
            binaryFile.seek(start);
            pageType = PageNodeType.get(binaryFile.readByte());
            binaryFile.readByte();
            noOfCells = binaryFile.readShort();
            startOffSet = binaryFile.readShort();
            remainSpace = startOffSet - 0x10 - (noOfCells * 2);

            rPage = binaryFile.readInt();

            parPageNum = binaryFile.readInt();

            binaryFile.readShort();

            if (pageType == PageNodeType.LEAF_TYPE)
                fillTableRecords();
            if (pageType == PageNodeType.INTERIOR_TYPE)
                fillLChild();
            if (pageType == PageNodeType.INTERIOR_INDEX || pageType == PageNodeType.LEAF_INDEX)
                fillIndexRecords();

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Exception while reading the page", ex);
        }
    }

    /**
     * Returns Index values
     *
     * @return List of String
     */
    public List<String> fetchIdxVal() {
        List<String> strIndexValues = new ArrayList<>();

        if (sIndexValues.size() > 0)
            strIndexValues.addAll(Arrays.asList(sIndexValues.toArray(new String[sIndexValues.size()])));
        if (lIndexValues.size() > 0) {
            Long[] lArray = lIndexValues.toArray(new Long[lIndexValues.size()]);
            for (Long aLong : lArray) {
                strIndexValues.add(aLong.toString());
            }
        }

        return strIndexValues;


    }

    public boolean isRoot() {
        return parPageNum == -1;
    }


    /**
     * Helper for operating on pages.
     *
     * @param file
     * @param pageNumber
     * @return Page Node Type
     * @throws IOException
     */
    public static PageNodeType getPageType(RandomAccessFile file, int pageNumber) throws IOException {
        try {
            int start = DBOperationsProcessor.pageSize * pageNumber;
            file.seek(start);
            return PageNodeType.get(file.readByte());
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Exception while obtaining the page type", ex);
            throw ex;
        }
    }

    /**
     * This function adds new pages to the table.
     *
     * @param file
     * @param pageType
     * @param rPage
     * @param parPageNum
     * @return
     */
    public static int addNewPage(RandomAccessFile file, PageNodeType pageType, int rPage, int parPageNum) {
        try {
            int pageNumber = Long.valueOf((file.length() / DBOperationsProcessor.pageSize)).intValue();
            file.setLength(file.length() + DBOperationsProcessor.pageSize);
            file.seek((long) DBOperationsProcessor.pageSize * pageNumber);
            file.write(pageType.getByteValue());
            file.write(0x00);
            file.writeShort(0);
            file.writeShort((short) (DBOperationsProcessor.pageSize));

            file.writeInt(rPage);
            file.writeInt(parPageNum);

            return pageNumber;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Exception while adding new page", ex);
            return -1;
        }
    }

    /**
     * Updates the values in specific table record
     *
     * @param record
     * @param ordinalPosition
     * @param newValue
     * @throws IOException
     */
    public void updateRecord(DataRecordForTable record, int ordinalPosition, Byte[] newValue) throws IOException {
        binaryFile.seek(start + record.offsetRcrd + 7);
        int voffset = 0;
        for (int i = 0; i < ordinalPosition; i++) {

            voffset += DBSupportedDataType.getLength(binaryFile.readByte());
        }

        //Setting the position in the file
        binaryFile.seek(start + record.offsetRcrd + 7 + record.colDtype.length + voffset);
        //Updating the record
        binaryFile.write(DBDatatypeConversionHelper.Bytestobytes(newValue));

    }

    /**
     * Helper to add new columns
     *
     * @param columnInfo
     */
    public void addNewColumn(ColumnValueConstrain columnInfo) {
        try {
            addTableRow(DBOperationsProcessor.columnsTable, Arrays.asList(new CellRecords(DBSupportedDataType.TEXT, columnInfo.tblName),
                    new CellRecords(DBSupportedDataType.TEXT, columnInfo.colname),
                    new CellRecords(DBSupportedDataType.TEXT, columnInfo.dataType.toString()),
                    new CellRecords(DBSupportedDataType.SMALLINT, columnInfo.ordPosition.toString()),
                    new CellRecords(DBSupportedDataType.TEXT, columnInfo.isNull ? "YES" : "NO"),
                    columnInfo.isPk ?
                            new CellRecords(DBSupportedDataType.TEXT, "PRI") : new CellRecords(DBSupportedDataType.NULL, "NULL"),
                    new CellRecords(DBSupportedDataType.TEXT, columnInfo.isSame ? "YES" : "NO")));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception while adding new column", e);
        }
    }

    /**
     * adds row and this method converts the attributes into byte array
     * <br> and calls addNewPageRecord
     *
     * @param tableName
     * @param attributes
     * @return
     * @throws IOException
     */
    public int addTableRow(String tableName, List<CellRecords> attributes) throws IOException {
        List<Byte> colDataTypes = new ArrayList<>();
        List<Byte> recordBody = new ArrayList<>();

        TableInfoHandler metaData = null;
        if (DBOperationsProcessor.dataStoreInitialized) {
            metaData = new TableInfoHandler(tableName);
            if (!metaData.validateInsert(attributes)) {
                return -1;
            }
        }

        for (CellRecords attribute : attributes) {
            //adding attribute to the record body
            recordBody.addAll(Arrays.asList(attribute.fieldValueByte));

            //Fill column Datatype for every attribute in the row
            if (attribute.dataType == DBSupportedDataType.TEXT) {
                colDataTypes.add(Integer.valueOf(DBSupportedDataType.TEXT.getValue() + (attribute.fieldValue.length())).byteValue());
            } else {
                colDataTypes.add(attribute.dataType.getValue());
            }
        }

        lastId++;

        short plsize = Integer.valueOf(recordBody.size() +
                colDataTypes.size() + 1).shortValue();

        List<Byte> reHeader = new ArrayList<>();

        reHeader.addAll(Arrays.asList(DBDatatypeConversionHelper.shortToBytes(plsize)));
        reHeader.addAll(Arrays.asList(DBDatatypeConversionHelper.intToBytes(lastId)));
        reHeader.add(Integer.valueOf(colDataTypes.size()).byteValue());
        reHeader.addAll(colDataTypes);

        addNewPageRecord(reHeader.toArray(new Byte[reHeader.size()]),
                recordBody.toArray(new Byte[recordBody.size()])
        );

        isRefreshed = true;
        if (DBOperationsProcessor.dataStoreInitialized) {
            metaData.recordsCount++;
            metaData.updateTableMetaData();
        }
        return lastId;
    }

    public List<DataRecordForTable> getPageRecords() {
        if (isRefreshed) {
            fillTableRecords();
        }
        isRefreshed = false;
        return records;
    }

    /**
     * Helper for deleting a record from Index
     *
     * @param recordIndex
     */
    private void DeletePageRecord(short recordIndex) {
        try {

            for (int i = recordIndex + 1; i < noOfCells; i++) {
                binaryFile.seek(start + 0x10 + (i * 2));
                short cStart = binaryFile.readShort();

                if (cStart == 0)
                    continue;

                binaryFile.seek(start + 0x10 + ((i - 1) * 2));
                binaryFile.writeShort(cStart);
            }

            noOfCells--;

            binaryFile.seek(start + 2);
            binaryFile.writeShort(noOfCells);

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception while deleting record from page", e);
        }
    }

    public void DeleteTableRecord(String tableName, short recordIndex) {
        DeletePageRecord(recordIndex);
        TableInfoHandler metaData = new TableInfoHandler(tableName);
        metaData.recordsCount--;
        metaData.updateTableMetaData();
        isRefreshed = true;

    }

    /**
     * adds a new record and updates the page header accordingly
     *
     * @param reHeader
     * @param recordBody
     * @throws IOException
     */
    private void addNewPageRecord(Byte[] reHeader, Byte[] recordBody) throws IOException {

        /* In case  there is no more free space available in the current page */
        if (reHeader.length + recordBody.length + 4 > remainSpace) {
            try {
                if (pageType == PageNodeType.LEAF_TYPE || pageType == PageNodeType.INTERIOR_TYPE) {
                    handleTableOverFlow();
                } else {
                    handleIndexOverflow();
                    return;
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Exception in handling table overflow");
            }
        }

        short cStart = startOffSet;
        short newcStart = Integer.valueOf((cStart - recordBody.length - reHeader.length - 2)).shortValue();
        binaryFile.seek((long) pageNumber * DBOperationsProcessor.pageSize + newcStart);

        binaryFile.write(DBDatatypeConversionHelper.Bytestobytes(reHeader));

        binaryFile.write(DBDatatypeConversionHelper.Bytestobytes(recordBody));

        binaryFile.seek(start + 0x10 + (noOfCells * 2));
        binaryFile.writeShort(newcStart);
        startOffSet = newcStart;

        binaryFile.seek(start + 4);
        binaryFile.writeShort(startOffSet);

        noOfCells++;
        binaryFile.seek(start + 2);
        binaryFile.writeShort(noOfCells);
        remainSpace = startOffSet - 0x10 - (noOfCells * 2);

    }


    /**
     * Helper for index overflow conditions
     *
     * @throws IOException
     */
    private void handleIndexOverflow() throws IOException {
        if (pageType == PageNodeType.LEAF_INDEX) {
            /*If the current page is the root*/
            if (parPageNum == -1) {
                parPageNum = addNewPage(binaryFile, PageNodeType.INTERIOR_INDEX, pageNumber, -1);
            }
            /*Handle creation of new left Page*/
            int newLeftLeafPageNumber = addNewPage(binaryFile, PageNodeType.LEAF_INDEX, pageNumber, parPageNum);

            setParent(parPageNum);

            /*Splitting record and inserting to left node*/
            IndexNode insertNodeTemp = this.insertNode;

            Page leftLeafPage = new Page(binaryFile, newLeftLeafPageNumber);
            IndexNode toInsertParentIndexNode = splitIndexRecordsBetweenPages(leftLeafPage);


            Page parentPage = new Page(binaryFile, parPageNum);

            /*based on the incoming index values shift the page*/
            int comparisonResult = WhereConditionProcessor.compare(insertNodeTemp.indexValue.fieldValue, toInsertParentIndexNode.indexValue.fieldValue, insertNode.indexValue.dataType);

            if (comparisonResult == 0) {
                toInsertParentIndexNode.rowId.addAll(insertNodeTemp.rowId);
                parentPage.addIndex(toInsertParentIndexNode, newLeftLeafPageNumber);
                shiftPage(parentPage);
                return;
            } else if (comparisonResult < 0) {
                leftLeafPage.addIndex(insertNodeTemp);
                shiftPage(leftLeafPage);
            } else {
                addIndex(insertNodeTemp);
            }

            parentPage.addIndex(toInsertParentIndexNode, newLeftLeafPageNumber);

        } else {
            /*multilevel splitting*/
            if (noOfCells < 3 && !idxPageCleaned) {
                idxPageCleaned = true;
                String[] indexValuesTemp = fetchIdxVal().toArray(new String[fetchIdxVal().size()]);
                HashMap<String, IndexRecord> pointerTemp = (HashMap<String, IndexRecord>) pointer.clone();
                IndexNode insertNodeTemp = this.insertNode;
                cleanPage();
                for (String s : indexValuesTemp) {
                    addIndex(pointerTemp.get(s).fetchIdxRowRfrnce(), pointerTemp.get(s).leftPageNumber);
                }

                addIndex(insertNodeTemp);
                return;
            }

            if (idxPageCleaned) {
                System.out.println("! Page overflow, increase the page size. Reached Max number of rows for an Index value");
                return;
            }


            if (parPageNum == -1) {
                parPageNum = addNewPage(binaryFile, PageNodeType.INTERIOR_INDEX, pageNumber, -1);
            }
            //creating a new interior page
            int newLeftInteriorPageNumber = addNewPage(binaryFile, PageNodeType.INTERIOR_INDEX, pageNumber, parPageNum);
            setParent(parPageNum);

            IndexNode insertNodeTemp = this.insertNode;
            Page leftInteriorPage = new Page(binaryFile, newLeftInteriorPageNumber);

            IndexNode toInsertParentIndexNode = splitIndexRecordsBetweenPages(leftInteriorPage);

            Page parentPage = new Page(binaryFile, parPageNum);
            int comparisonResult = WhereConditionProcessor.compare(insertNodeTemp.indexValue.fieldValue, toInsertParentIndexNode.indexValue.fieldValue, insertNode.indexValue.dataType);

            /*adding the Orphan in the middle to the left page*/
            Page middleOrphan = new Page(binaryFile, toInsertParentIndexNode.leftPageNumber);
            middleOrphan.setParent(parPageNum);
            leftInteriorPage.setrPageNumber(middleOrphan.pageNumber);

            if (comparisonResult == 0) {
                toInsertParentIndexNode.rowId.addAll(insertNodeTemp.rowId);
                parentPage.addIndex(toInsertParentIndexNode, newLeftInteriorPageNumber);
                shiftPage(parentPage);
                return;
            } else if (comparisonResult < 0) {
                leftInteriorPage.addIndex(insertNodeTemp);
                shiftPage(leftInteriorPage);
            } else {
                addIndex(insertNodeTemp);
            }

            parentPage.addIndex(toInsertParentIndexNode, newLeftInteriorPageNumber);

        }


    }


    /**
     * clean the current page by resetting the offsets of the page and the number of
     * <br> records,
     *
     * @throws IOException
     */
    private void cleanPage() throws IOException {

        noOfCells = 0;
        startOffSet = Long.valueOf(DBOperationsProcessor.pageSize).shortValue();
        remainSpace = startOffSet - 0x10 - (noOfCells * 2);
        byte[] emptyBytes = new byte[512 - 16];
        Arrays.fill(emptyBytes, (byte) 0);
        binaryFile.seek(start + 16);
        binaryFile.write(emptyBytes);
        binaryFile.seek(start + 2);
        binaryFile.writeShort(noOfCells);
        binaryFile.seek(start + 4);
        binaryFile.writeShort(startOffSet);
        lIndexValues = new TreeSet<>();
        sIndexValues = new TreeSet<>();
        pointer = new HashMap<>();

    }

    /**
     * copies left half to current right page and return the middle Index node required to add to parent
     *
     * @param newLeftPage
     * @return
     * @throws IOException
     */
    private IndexNode splitIndexRecordsBetweenPages(Page newLeftPage) throws IOException {

        try {
            int mid = fetchIdxVal().size() / 2;
            String[] indexValuesTemp = fetchIdxVal().toArray(new String[fetchIdxVal().size()]);

            IndexNode toInsertParentIndexNode = pointer.get(indexValuesTemp[mid]).fetchIdxRowRfrnce();
            toInsertParentIndexNode.leftPageNumber = pointer.get(indexValuesTemp[mid]).leftPageNumber;

            HashMap<String, IndexRecord> pointerTemp = (HashMap<String, IndexRecord>) pointer.clone();

            for (int i = 0; i < mid; i++) {
                newLeftPage.addIndex(pointerTemp.get(indexValuesTemp[i]).fetchIdxRowRfrnce(), pointerTemp.get(indexValuesTemp[i]).leftPageNumber);
            }

            cleanPage();
            sIndexValues = new TreeSet<>();
            lIndexValues = new TreeSet<>();
            pointer = new HashMap<>();

            for (int i = mid + 1; i < indexValuesTemp.length; i++) {
                addIndex(pointerTemp.get(indexValuesTemp[i]).fetchIdxRowRfrnce(), pointerTemp.get(indexValuesTemp[i]).leftPageNumber);
            }

            return toInsertParentIndexNode;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception while splitting index page for insert", e);
            throw e;
        }

    }

    /**
     * This method deals with table over flow
     *
     * @throws IOException
     */
    private void handleTableOverFlow() throws IOException {
        if (pageType == PageNodeType.LEAF_TYPE) {
            int newRightLeafPageNumber = addNewPage(binaryFile, pageType, -1, -1);
            if (parPageNum == -1) {


                int newParPageNum = addNewPage(binaryFile, PageNodeType.INTERIOR_TYPE,
                        newRightLeafPageNumber, -1);

                setrPageNumber(newRightLeafPageNumber);
                setParent(newParPageNum);

                Page newParentPage = new Page(binaryFile, newParPageNum);
                newParPageNum = newParentPage.addLeftTableChild(pageNumber, lastId);

                newParentPage.setrPageNumber(newRightLeafPageNumber);


                Page newLeafPage = new Page(binaryFile, newRightLeafPageNumber);
                newLeafPage.setParent(newParPageNum);


                shiftPage(newLeafPage);
            } else {

                Page parentPage = new Page(binaryFile, parPageNum);
                parPageNum = parentPage.addLeftTableChild(pageNumber, lastId);


                parentPage.setrPageNumber(newRightLeafPageNumber);


                setrPageNumber(newRightLeafPageNumber);


                Page newLeafPage = new Page(binaryFile, newRightLeafPageNumber);
                newLeafPage.setParent(parPageNum);

                shiftPage(newLeafPage);
            }
        } else {

            int newRightLeafPageNumber = addNewPage(binaryFile, pageType, -1, -1);
            int newparPageNum = addNewPage(binaryFile, PageNodeType.INTERIOR_TYPE,
                    newRightLeafPageNumber, -1);

            setrPageNumber(newRightLeafPageNumber);
            setParent(newparPageNum);
            Page newParentPage = new Page(binaryFile, newparPageNum);
            newparPageNum = newParentPage.addLeftTableChild(pageNumber, lastId);

            newParentPage.setrPageNumber(newRightLeafPageNumber);
            Page newLeafPage = new Page(binaryFile, newRightLeafPageNumber);
            newLeafPage.setParent(newparPageNum);


            shiftPage(newLeafPage);
        }
    }

    /**
     * Adds the left child node to current page
     *
     * @param lCpageNum
     * @param rowId
     * @return
     * @throws IOException
     */
    private int addLeftTableChild(int lCpageNum, int rowId) throws IOException {
        for (InternalTableRecord intRecord : lChild) {
            if (intRecord.rowId == rowId)
                return pageNumber;
        }
        if (pageType == PageNodeType.INTERIOR_TYPE) {
            List<Byte> recordBody = new ArrayList<>();

            List<Byte> reHeader = new ArrayList<>(Arrays.asList(DBDatatypeConversionHelper.intToBytes(lCpageNum)));
            recordBody.addAll(Arrays.asList(DBDatatypeConversionHelper.intToBytes(rowId)));

            addNewPageRecord(reHeader.toArray(new Byte[reHeader.size()]),
                    recordBody.toArray(new Byte[recordBody.size()]));
        }
        return pageNumber;

    }

    /**
     * copy all the members from the newly created page to the present page
     *
     * @param newPage
     */
    private void shiftPage(Page newPage) {
        pageType = newPage.pageType;
        noOfCells = newPage.noOfCells;
        pageNumber = newPage.pageNumber;
        startOffSet = newPage.startOffSet;
        rPage = newPage.rPage;
        parPageNum = newPage.parPageNum;
        lChild = newPage.lChild;
        sIndexValues = newPage.sIndexValues;
        lIndexValues = newPage.lIndexValues;
        pointer = newPage.pointer;
        records = newPage.records;
        start = newPage.start;
        remainSpace = newPage.remainSpace;
    }

    /**
     * sets the parPageNum as parent for the current page
     *
     * @param parPageNum
     * @throws IOException
     */
    public void setParent(int parPageNum) throws IOException {
        binaryFile.seek((long) DBOperationsProcessor.pageSize * pageNumber + 0x0A);
        binaryFile.writeInt(parPageNum);
        this.parPageNum = parPageNum;
    }

    /**
     * Sets the right page number
     *
     * @param rPageNumber
     * @throws IOException
     */
    public void setrPageNumber(int rPageNumber) throws IOException {
        binaryFile.seek((long) DBOperationsProcessor.pageSize * pageNumber + 0x06);
        binaryFile.writeInt(rPageNumber);
        this.rPage = rPageNumber;
    }

    /**
     * Deletes a node in the index
     *
     * @param node
     */
    public void DeleteIndex(IndexNode node) {
        DeletePageRecord(pointer.get(node.indexValue.fieldValue).headerIndex);
        fillIndexRecords();
        refreshHeaderOffset();
    }

    /**
     * Adds a new Index Node
     *
     * @param node
     * @throws IOException
     */
    public void addIndex(IndexNode node) throws IOException {
        addIndex(node, -1);
    }

    public void addIndex(IndexNode node, int leftpageNumber) throws IOException {
        insertNode = node;
        insertNode.leftPageNumber = leftpageNumber;
        List<Integer> rowIds = new ArrayList<>();

        List<String> ixValues = fetchIdxVal();
        if (fetchIdxVal().contains(node.indexValue.fieldValue)) {
            leftpageNumber = pointer.get(node.indexValue.fieldValue).leftPageNumber;
            insertNode.leftPageNumber = leftpageNumber;
            rowIds = pointer.get(node.indexValue.fieldValue).rowIdList;
            rowIds.addAll(insertNode.rowId);
            insertNode.rowId = rowIds;
            DeletePageRecord(pointer.get(node.indexValue.fieldValue).headerIndex);
            if (inType == DBSupportedDataType.TEXT || inType == null || node.indexValue.fieldValue.toUpperCase().equals("NULL"))
                sIndexValues.remove(node.indexValue.fieldValue);
            else
                lIndexValues.remove(Long.parseLong(node.indexValue.fieldValue));
        }

        rowIds.addAll(node.rowId);

        rowIds = new ArrayList<>(new HashSet<>(rowIds));

        List<Byte> recordHead = new ArrayList<>();

        List<Byte> recordBody = new ArrayList<>(Arrays.asList(Integer.valueOf(rowIds.size()).byteValue()));

        if (node.indexValue.dataType == DBSupportedDataType.TEXT)
            recordBody.add(Integer.valueOf(node.indexValue.dataType.getValue()
                    + node.indexValue.fieldValue.length()).byteValue());
        else
            recordBody.add(node.indexValue.dataType.getValue());

        recordBody.addAll(Arrays.asList(node.indexValue.fieldValueByte));

        for (Integer rowId : rowIds) {
            recordBody.addAll(Arrays.asList(DBDatatypeConversionHelper.intToBytes(rowId)));
        }

        short payload = Integer.valueOf(recordBody.size()).shortValue();
        if (pageType == PageNodeType.INTERIOR_INDEX)
            recordHead.addAll(Arrays.asList(DBDatatypeConversionHelper.intToBytes(leftpageNumber)));

        recordHead.addAll(Arrays.asList(DBDatatypeConversionHelper.shortToBytes(payload)));

        addNewPageRecord(recordHead.toArray(new Byte[recordHead.size()]),
                recordBody.toArray(new Byte[recordBody.size()])
        );

        fillIndexRecords();
        refreshHeaderOffset();

    }

    /**
     * Resets the header offset
     */
    private void refreshHeaderOffset() {
        try {
            binaryFile.seek(start + 0x10);
            for (String indexVal : fetchIdxVal()) {
                binaryFile.writeShort(pointer.get(indexVal).offsetOfPage);
            }

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Exception during refreshing header offset", ex);
        }
    }

    /**
     * Helps to fill the list of rows inside the Page to a list object
     */
    private void fillTableRecords() {
        short plsize;
        byte noOfColumns;
        records = new ArrayList<>();
        map = new HashMap<>();
        try {
            for (short i = 0; i < noOfCells; i++) {
                binaryFile.seek(start + 0x10 + (i * 2));
                short cStart = binaryFile.readShort();
                if (cStart == 0)
                    continue;
                binaryFile.seek(start + cStart);

                plsize = binaryFile.readShort();
                int rowId = binaryFile.readInt();
                noOfColumns = binaryFile.readByte();

                if (lastId < rowId) lastId = rowId;

                byte[] colDatatypes = new byte[noOfColumns];
                byte[] recordBody = new byte[plsize - noOfColumns - 1];

                binaryFile.read(colDatatypes);
                binaryFile.read(recordBody);

                DataRecordForTable record = new DataRecordForTable(i, rowId, cStart
                        , colDatatypes, recordBody);
                records.add(record);
                map.put(rowId, record);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception filling records from the page", e);
        }
    }


    /**
     * If interior page has no space fills the left children
     */
    private void fillLChild() {
        try {
            lChild = new ArrayList<>();

            int lCPageNum;
            int rowId;
            for (int i = 0; i < noOfCells; i++) {
                binaryFile.seek(start + 0x10 + (i * 2));
                short cStart = binaryFile.readShort();
                if (cStart == 0)//ignore deleted cells
                    continue;
                binaryFile.seek(start + cStart);

                lCPageNum = binaryFile.readInt();
                rowId = binaryFile.readInt();
                lChild.add(new InternalTableRecord(rowId, lCPageNum));
            }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Exception filling records from the page" ,ex);
        }

    }

    /**
     * Poupulates the index records
     */
    private void fillIndexRecords() {
        try {
            lIndexValues = new TreeSet<>();
            sIndexValues = new TreeSet<>();
            pointer = new HashMap<>();

            int leftPageNumber = -1;
            byte noOfRowIds;
            byte dataType;
            for (short i = 0; i < noOfCells; i++) {
                binaryFile.seek(start + 0x10 + (i * 2));
                short cStart = binaryFile.readShort();
                if (cStart == 0)//ignore deleted cells
                    continue;
                binaryFile.seek(start + cStart);

                if (pageType == PageNodeType.INTERIOR_INDEX)
                    leftPageNumber = binaryFile.readInt();

                short payload = binaryFile.readShort();

                noOfRowIds = binaryFile.readByte();
                dataType = binaryFile.readByte();

                if (inType == null && DBSupportedDataType.get(dataType) != DBSupportedDataType.NULL)
                    inType = DBSupportedDataType.get(dataType);

                byte[] indexValue = new byte[DBSupportedDataType.getLength(dataType)];
                binaryFile.read(indexValue);

                List<Integer> lstRowIds = new ArrayList<>();
                for (int j = 0; j < noOfRowIds; j++) {
                    lstRowIds.add(binaryFile.readInt());
                }

                IndexRecord record = new IndexRecord(i, DBSupportedDataType.get(dataType), noOfRowIds, indexValue
                        , lstRowIds, leftPageNumber, rPage, pageNumber, cStart);

                if (inType == DBSupportedDataType.TEXT || inType == null || record.fetchIdxRowRfrnce().indexValue.fieldValue.toUpperCase().equals("NULL"))
                    sIndexValues.add(record.fetchIdxRowRfrnce().indexValue.fieldValue);
                else
                    lIndexValues.add(Long.parseLong(record.fetchIdxRowRfrnce().indexValue.fieldValue));

                pointer.put(record.fetchIdxRowRfrnce().indexValue.fieldValue, record);

            }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Exception while filling records from the page" ,ex);
        }
    }

}
