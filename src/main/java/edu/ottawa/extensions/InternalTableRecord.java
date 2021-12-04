package edu.ottawa.extensions;


//This is a holder record of type interior record for a table
public class InternalTableRecord {
    public int rowId;
    public int leftPageNumber;

    public InternalTableRecord(int rowId, int leftPageNumber) {
        this.rowId = rowId;
        this.leftPageNumber = leftPageNumber;
    }

}
