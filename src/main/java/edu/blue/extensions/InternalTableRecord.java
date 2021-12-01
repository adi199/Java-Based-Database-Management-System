package edu.blue.extensions;

/**
 * A holder record of type Interior Record for a table
 *
 * @author Team Blue
 */
public class InternalTableRecord {
    public int rowId;
    public int leftPageNumber;

    public InternalTableRecord(int rowId, int leftPageNumber) {
        this.rowId = rowId;
        this.leftPageNumber = leftPageNumber;
    }

}
