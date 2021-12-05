package edu.ottawa.extensions;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


// Implementation of BTree
public class BTree {

    private static final Logger lgr = Logger.getLogger(BTree.class.getName());

    Page root;
    RandomAccessFile binaryFile;

    public BTree(RandomAccessFile file) {
        this.binaryFile = file;
        this.root = new Page(binaryFile, DBOperationsProcessor.getRootPageNumber(binaryFile));
    }

    // Helps in finding correct page number to insert index value by performing BinarySearch
    private int fetchNearestPgNumber(Page page, String idxVal) {
        if (page.pageType == PageNodeType.LEAF_INDEX) {
            return page.pageNumber;
        } else {
            if (WhereConditionProcessor.compare(idxVal, page.fetchIdxVal().get(0), page.inType) < 0)
                return fetchNearestPgNumber
                        (new Page(binaryFile, page.pointer.get(page.fetchIdxVal().get(0)).leftPageNumber),
                                idxVal);
            else if (WhereConditionProcessor.compare(idxVal, page.fetchIdxVal().get(page.fetchIdxVal().size() - 1), page.inType) > 0)
                return fetchNearestPgNumber(
                        new Page(binaryFile, page.rPage),
                        idxVal);
            else {
                //perform binary search
                String closeVal = bnrySearch(page.fetchIdxVal().toArray(new String[page.fetchIdxVal().size()]), idxVal, 0, page.fetchIdxVal().size() - 1, page.inType);
                int i = page.fetchIdxVal().indexOf(closeVal);
                List<String> indexValues = page.fetchIdxVal();
                if (closeVal.compareTo(idxVal) < 0 && i + 1 < indexValues.size()) {
                    return page.pointer.get(indexValues.get(i + 1)).leftPageNumber;
                } else if (closeVal.compareTo(idxVal) > 0) {
                    return page.pointer.get(closeVal).leftPageNumber;
                } else {
                    return page.pageNumber;
                }
            }
        }
    }

    // Returns matched rows
    public List<Integer> fetchRowId(WhereConditionProcessor condition) {
        List<Integer> rId = new ArrayList<>();

        //find nearest page number for given condition*/
        Page page = new Page(binaryFile, fetchNearestPgNumber(root, condition.comparisonValue));
        String[] idxVal = page.fetchIdxVal().toArray(new String[page.fetchIdxVal().size()]);
        SupportedOperators oprtnTyp = condition.getOperation();

        for (String indexValue : idxVal) {
            if (condition.checkCondition(page.pointer.get(indexValue).fetchIdxRowRfrnce().indexValue.fieldValue))
                rId.addAll(page.pointer.get(indexValue).rowIdList);
        }

        if (oprtnTyp == SupportedOperators.LESS_THAN || oprtnTyp == SupportedOperators.LESS_THAN_OR_EQUAL) {
            if (page.pageType == PageNodeType.LEAF_INDEX)
                rId.addAll(fetchAllLeftRIds(page.parPageNum, idxVal[0]));
            else
                rId.addAll(fetchAllLeftRIds(page.pageNumber, condition.comparisonValue));
        }

        if (oprtnTyp == SupportedOperators.GREATER_THAN || oprtnTyp == SupportedOperators.GREATER_THAN_OR_EQUAL) {
            if (page.pageType == PageNodeType.LEAF_INDEX)
                rId.addAll(fetchAllRightRids(page.parPageNum, idxVal[idxVal.length - 1]));
            else
                rId.addAll(fetchAllRightRids(page.pageNumber, condition.comparisonValue));
        }

        return rId;
    }

    // Returns rowid's to the left of given value
    private List<Integer> fetchAllLeftRIds(int pgNbr, String idxVal) {
        List<Integer> rIds = new ArrayList<>();
        if (pgNbr == -1)
            return rIds;
        Page page = new Page(this.binaryFile, pgNbr);
        List<String> indexValues = Arrays.asList(page.fetchIdxVal().toArray(new String[page.fetchIdxVal().size()]));


        for (int i = 0; i < indexValues.size() && WhereConditionProcessor.compare(indexValues.get(i), idxVal, page.inType) < 0; i++) {

            rIds.addAll(page.pointer.get(indexValues.get(i)).fetchIdxRowRfrnce().rowId);
            insertChildRIds(page.pointer.get(indexValues.get(i)).leftPageNumber, rIds);

        }

        if (page.pointer.get(idxVal) != null)
            insertChildRIds(page.pointer.get(idxVal).leftPageNumber, rIds);


        return rIds;
    }

    // Returns rowid's to the right of given value
    private List<Integer> fetchAllRightRids(int pageNumber, String indexValue) {

        List<Integer> rowIds = new ArrayList<>();

        if (pageNumber == -1)
            return rowIds;
        Page page = new Page(this.binaryFile, pageNumber);
        List<String> indexValues = Arrays.asList(page.fetchIdxVal().toArray(new String[page.fetchIdxVal().size()]));
        for (int i = indexValues.size() - 1; i >= 0 && WhereConditionProcessor.compare(indexValues.get(i), indexValue, page.inType) > 0; i--) {
            rowIds.addAll(page.pointer.get(indexValues.get(i)).fetchIdxRowRfrnce().rowId);
            insertChildRIds(page.rPage, rowIds);
        }

        if (page.pointer.get(indexValue) != null)
            insertChildRIds(page.pointer.get(indexValue).rightPageNumber, rowIds);

        return rowIds;
    }

    // saves the child rows ids
    private void insertChildRIds(int pageNumber, List<Integer> rowIds) {
        if (pageNumber == -1)
            return;
        Page page = new Page(this.binaryFile, pageNumber);
        for (IndexRecord rcrd : page.pointer.values()) {
            rowIds.addAll(rcrd.rowIdList);
            if (page.pageType == PageNodeType.INTERIOR_INDEX) {
                insertChildRIds(rcrd.leftPageNumber, rowIds);
                insertChildRIds(rcrd.rightPageNumber, rowIds);
            }
        }
    }

    // inserts values into records
    public void insrt(CellRecords columns, List<Integer> rIds) {
        try {
            int pageNumber = fetchNearestPgNumber(root, columns.fieldValue);
            Page page = new Page(binaryFile, pageNumber);
            page.addIndex(new IndexNode(columns, rIds));
        } catch (IOException e) {
            lgr.log(Level.SEVERE, "Exception inserting to index", e);
        }
    }

    // Insert entry to the index page
    public void insrt(CellRecords attribute, int rowId) {
        insrt(attribute, Arrays.asList(rowId));
    }

    //deletes the record
    public void delete(CellRecords column, int rId) {

        try {
            int pgNbr = fetchNearestPgNumber(root, column.fieldValue);
            Page page = new Page(binaryFile, pgNbr);

            IndexNode auxillaryNode = page.pointer.get(column.fieldValue).fetchIdxRowRfrnce();
            auxillaryNode.rowId.remove(auxillaryNode.rowId.indexOf(rId));

            page.DeleteIndex(auxillaryNode);
            if (auxillaryNode.rowId.size() != 0)
                page.addIndex(auxillaryNode);

        } catch (IOException e) {
            lgr.log(Level.SEVERE, "Exception while deleting from index", e);
        }

    }

    //Searches the table using BinarySearch
    private String bnrySearch(String[] val, String fetchVal, int start, int end, DBSupportedDataType dType) {

        if (end - start <= 3) {
            int i;
            for (i = start; i < end; i++) {
                if (WhereConditionProcessor.compare(val[i], fetchVal, dType) < 0)
                    continue;
                else
                    break;
            }
            return val[i];
        } else {

            int midPoint = (end - start) / 2 + start;
            if (val[midPoint].equals(fetchVal))
                return val[midPoint];

            if (WhereConditionProcessor.compare(val[midPoint], fetchVal, dType) < 0)
                return bnrySearch(val, fetchVal, midPoint + 1, end, dType);
            else
                return bnrySearch(val, fetchVal, start, midPoint - 1, dType);

        }

    }


}