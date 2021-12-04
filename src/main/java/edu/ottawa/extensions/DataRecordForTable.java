package edu.ottawa.extensions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// This class has methods for converting record data into byte array
public class DataRecordForTable {
    public int rId;
    public Byte[] colDtype;
    public Byte[] recordInformation;
    private List<CellRecords> columnsList;
    public short offsetRcrd;
    public short idxPageHd;

    DataRecordForTable(short idxPageHd, int rId, short offsetRcrd, byte[] colDtype, byte[] recordInformation) {
        this.rId = rId;
        this.recordInformation = DBDatatypeConversionHelper.byteToBytes(recordInformation);
        this.colDtype = DBDatatypeConversionHelper.byteToBytes(colDtype);
        this.offsetRcrd = offsetRcrd;
        this.idxPageHd = idxPageHd;
        assignAttrb();
    }

    public List<CellRecords> getColumnsList() {
        return columnsList;
    }

    // Attributes setup
    private void assignAttrb() {
        columnsList = new ArrayList<>();
        int rfrnce = 0;
        for (Byte colDataType : colDtype) {
            byte[] colVal = DBDatatypeConversionHelper.Bytestobytes(Arrays.copyOfRange(recordInformation, rfrnce, rfrnce + DBSupportedDataType.getLength(colDataType)));
            columnsList.add(new CellRecords(DBSupportedDataType.get(colDataType), colVal));
            rfrnce = rfrnce + DBSupportedDataType.getLength(colDataType);
        }
    }

}
