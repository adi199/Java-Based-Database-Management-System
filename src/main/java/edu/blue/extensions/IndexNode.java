package edu.blue.extensions;

import java.util.List;

/**
 * Node class in a holder for Indexes
 *
 * @author Team Blue
 */
public class IndexNode {

    public List<Integer> rowId;
    public int leftPageNumber;
    public CellRecords indexValue;

    public IndexNode(CellRecords indexValue, List<Integer> rowId) {
        this.indexValue = indexValue;
        this.rowId = rowId;
    }

}