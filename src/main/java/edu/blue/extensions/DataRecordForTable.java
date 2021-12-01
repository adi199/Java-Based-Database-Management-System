package edu.blue.extensions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This Helper class contains methods for converting the table <br>
 * record data header and body into byte array
 *
 * @author Team Blue
 */
public class DataRecordForTable {
    public int rowId;
    public Byte[] columnDataTypes;
    public Byte[] recordContent;
    private List<CellRecords> attributeList;
    public short OffsetofRecord;
    public short IndexPageHead;

    DataRecordForTable(short IndexPageHead, int rowId, short OffsetofRecord, byte[] columnDataTypes, byte[] recordContent) {
        this.rowId = rowId;
        this.recordContent = DBDatatypeConversionHelper.byteToBytes(recordContent);
        this.columnDataTypes = DBDatatypeConversionHelper.byteToBytes(columnDataTypes);
        this.OffsetofRecord = OffsetofRecord;
        this.IndexPageHead = IndexPageHead;
        setAttributes();
    }

    public List<CellRecords> getAttributeList() {
        return attributeList;
    }

    /**
     * Sets the attributes.
     */
    private void setAttributes() {
        attributeList = new ArrayList<>();
        int reference = 0;
        for (Byte colDataType : columnDataTypes) {
            byte[] fieldValue = DBDatatypeConversionHelper.Bytestobytes(Arrays.copyOfRange(recordContent, reference, reference + DBSupportedDataType.getLength(colDataType)));
            attributeList.add(new CellRecords(DBSupportedDataType.get(colDataType), fieldValue));
            reference = reference + DBSupportedDataType.getLength(colDataType);
        }
    }

}
