package edu.ottawa.extensions;

import java.util.List;


//This is used as a holder to store index records.
public class IndexRecord {
    public Byte numberOfRowIds;
    public DBSupportedDataType dataType;
    public Byte[] indexRecordValue;
    public List<Integer> rowIdList;
    int leftPageNumber;
    int rightPageNumber;
    int pageNumber;
    public short headerIndex;
    public short offsetOfPage;
    private final IndexNode indexNodeReference;


    IndexRecord(short headerIndex, DBSupportedDataType dataType, Byte numberOfRowIds, byte[] indexRecordValue, List<Integer> rowIdList
            , int leftPageNumber, int rightPageNumber, int pageNumber, short offsetOfPage) {

        this.dataType = dataType;
        this.indexRecordValue = DBDatatypeConversionHelper.byteToBytes(indexRecordValue);
        this.rowIdList = rowIdList;
        this.offsetOfPage = offsetOfPage;
        this.headerIndex = headerIndex;
        this.numberOfRowIds = numberOfRowIds;

        indexNodeReference = new IndexNode(new CellRecords(this.dataType, indexRecordValue), rowIdList);
        this.leftPageNumber = leftPageNumber;
        this.rightPageNumber = rightPageNumber;
        this.pageNumber = pageNumber;
    }

    public IndexNode fetchIdxRowRfrnce() {
        return indexNodeReference;
    }


}