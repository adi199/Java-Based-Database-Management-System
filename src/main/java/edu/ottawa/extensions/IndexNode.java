package edu.ottawa.extensions;

import java.util.List;

//This is a node class in a holder for Indexes
public class IndexNode {

    public List<Integer> rowId;
    public CellRecords indexValue;
    public int leftPageNumber;

    public IndexNode(CellRecords indexValue, List<Integer> rowId) {
        this.indexValue = indexValue;
        this.rowId = rowId;
    }

}