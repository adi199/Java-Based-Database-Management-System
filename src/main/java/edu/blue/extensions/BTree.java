package edu.blue.extensions;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * B Tree Implementation
 *
 * @author Team Blue
 */
public class BTree {

    private static final Logger LOGGER = Logger.getLogger(BTree.class.getName());

    Page root;
    RandomAccessFile binaryFile;

    public BTree(RandomAccessFile file) {
        this.binaryFile = file;
        this.root = new Page(binaryFile, DBOperationsProcessor.getRootPageNumber(binaryFile));
    }

    /**
     * Perform recursive BinarySearch using given value and find right page number for inserting the index value
     *
     * @param page
     * @param indexValue
     * @return
     */
    private int getClosestpageNumber(Page page, String indexValue) {
        if (page.pageType == PageNodeType.LEAF_INDEX) {
            return page.pageNumber;
        } else {
            if (WhereConditionProcessor.compare(indexValue, page.getIndexValues().get(0), page.inType) < 0)
                return getClosestpageNumber
                        (new Page(binaryFile, page.pointer.get(page.getIndexValues().get(0)).leftPageNumber),
                                indexValue);
            else if (WhereConditionProcessor.compare(indexValue, page.getIndexValues().get(page.getIndexValues().size() - 1), page.inType) > 0)
                return getClosestpageNumber(
                        new Page(binaryFile, page.rPage),
                        indexValue);
            else {
                //perform binary search
                String nearValue = binarySearch(page.getIndexValues().toArray(new String[page.getIndexValues().size()]), indexValue, 0, page.getIndexValues().size() - 1, page.inType);
                int i = page.getIndexValues().indexOf(nearValue);
                List<String> indexValues = page.getIndexValues();
                if (nearValue.compareTo(indexValue) < 0 && i + 1 < indexValues.size()) {
                    return page.pointer.get(indexValues.get(i + 1)).leftPageNumber;
                } else if (nearValue.compareTo(indexValue) > 0) {
                    return page.pointer.get(nearValue).leftPageNumber;
                } else {
                    return page.pageNumber;
                }
            }
        }
    }

    /**
     * Returns the list of rows matching condition
     *
     * @param condition
     * @return
     */
    public List<Integer> getRowIds(WhereConditionProcessor condition) {
        List<Integer> rowIds = new ArrayList<>();

        /*obtain the nearest page number satisfying the condition*/
        Page page = new Page(binaryFile, getClosestpageNumber(root, condition.comparisonValue));
        String[] indexValues = page.getIndexValues().toArray(new String[page.getIndexValues().size()]);
        SupportedOperators operationType = condition.getOperation();

        /*Save  the Row Ids if the index Values is ame as the closest value*/
        for (String indexValue : indexValues) {
            if (condition.checkCondition(page.pointer.get(indexValue).getIndexNodeReference().indexValue.fieldValue))
                rowIds.addAll(page.pointer.get(indexValue).rowIdList);
        }

        /* Save all the Row Ids Recursively from the left side of the node*/
        if (operationType == SupportedOperators.LESSTHAN || operationType == SupportedOperators.LESSTHANOREQUAL) {
            if (page.pageType == PageNodeType.LEAF_INDEX)
                rowIds.addAll(getAllRowIdsLeftOf(page.parPageNum, indexValues[0]));
            else
                rowIds.addAll(getAllRowIdsLeftOf(page.pageNumber, condition.comparisonValue));
        }

        if (operationType == SupportedOperators.GREATERTHAN || operationType == SupportedOperators.GREATERTHANOREQUAL) {
            if (page.pageType == PageNodeType.LEAF_INDEX)
                rowIds.addAll(getAllRowIdsRightOf(page.parPageNum, indexValues[indexValues.length - 1]));
            else
                rowIds.addAll(getAllRowIdsRightOf(page.pageNumber, condition.comparisonValue));
        }

        return rowIds;
    }

    /**
     * Returns all the rowIds left of given value
     *
     * @param pageNumber
     * @param indexValue
     * @return
     */
    private List<Integer> getAllRowIdsLeftOf(int pageNumber, String indexValue) {
        List<Integer> rowIds = new ArrayList<>();
        if (pageNumber == -1)
            return rowIds;
        Page page = new Page(this.binaryFile, pageNumber);
        List<String> indexValues = Arrays.asList(page.getIndexValues().toArray(new String[page.getIndexValues().size()]));


        for (int i = 0; i < indexValues.size() && WhereConditionProcessor.compare(indexValues.get(i), indexValue, page.inType) < 0; i++) {

            rowIds.addAll(page.pointer.get(indexValues.get(i)).getIndexNodeReference().rowId);
            addAllChildRowIds(page.pointer.get(indexValues.get(i)).leftPageNumber, rowIds);

        }

        if (page.pointer.get(indexValue) != null)
            addAllChildRowIds(page.pointer.get(indexValue).leftPageNumber, rowIds);


        return rowIds;
    }

    /**
     * Returns all the rowIds right of given value
     *
     * @param pageNumber
     * @param indexValue
     * @return
     */
    private List<Integer> getAllRowIdsRightOf(int pageNumber, String indexValue) {

        List<Integer> rowIds = new ArrayList<>();

        if (pageNumber == -1)
            return rowIds;
        Page page = new Page(this.binaryFile, pageNumber);
        List<String> indexValues = Arrays.asList(page.getIndexValues().toArray(new String[page.getIndexValues().size()]));
        for (int i = indexValues.size() - 1; i >= 0 && WhereConditionProcessor.compare(indexValues.get(i), indexValue, page.inType) > 0; i--) {
            rowIds.addAll(page.pointer.get(indexValues.get(i)).getIndexNodeReference().rowId);
            //        System.out.println(Arrays.toString(rowIds.toArray()));
            addAllChildRowIds(page.rPage, rowIds);
            //           System.out.println(Arrays.toString(rowIds.toArray()));
        }

        if (page.pointer.get(indexValue) != null)
            addAllChildRowIds(page.pointer.get(indexValue).rightPageNumber, rowIds);

        return rowIds;
    }

    /**
     * Save all child row ids
     *
     * @param pageNumber
     * @param rowIds
     */
    private void addAllChildRowIds(int pageNumber, List<Integer> rowIds) {
        if (pageNumber == -1)
            return;
        Page page = new Page(this.binaryFile, pageNumber);
        for (IndexRecord record : page.pointer.values()) {
            rowIds.addAll(record.rowIdList);
            if (page.pageType == PageNodeType.INTERIOR_INDEX) {
                addAllChildRowIds(record.leftPageNumber, rowIds);
                addAllChildRowIds(record.rightPageNumber, rowIds);
            }
        }
    }

    /**
     * Helper for Inserting the values
     *
     * @param attribute
     * @param rowIds
     */
    public void insert(CellRecords attribute, List<Integer> rowIds) {
        try {
            int pageNumber = getClosestpageNumber(root, attribute.fieldValue);
            Page page = new Page(binaryFile, pageNumber);
            page.addIndex(new IndexNode(attribute, rowIds));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception inserting to index", e);
        }
    }

    /**
     * Insert index value in the Index Page
     *
     * @param attribute
     * @param rowId
     */
    public void insert(CellRecords attribute, int rowId) {
        insert(attribute, Arrays.asList(rowId));
    }

    /**
     * Deletes the specified row
     *
     * @param attribute
     * @param rowid
     */
    public void delete(CellRecords attribute, int rowid) {

        try {
            int pageNumber = getClosestpageNumber(root, attribute.fieldValue);
            Page page = new Page(binaryFile, pageNumber);

            IndexNode auxillaryNode = page.pointer.get(attribute.fieldValue).getIndexNodeReference();
            //remove the rowid from the index value
            auxillaryNode.rowId.remove(auxillaryNode.rowId.indexOf(rowid));

            page.DeleteIndex(auxillaryNode);
            if (auxillaryNode.rowId.size() != 0)
                page.addIndex(auxillaryNode);

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception while deleting from index", e);
        }

    }

    /**
     * Binary Search the tableRecord
     *
     * @param values
     * @param searchValue
     * @param start
     * @param end
     * @param dataType
     * @return
     */
    private String binarySearch(String[] values, String searchValue, int start, int end, DBSupportedDataType dataType) {

        if (end - start <= 3) {
            int i;
            for (i = start; i < end; i++) {
                if (WhereConditionProcessor.compare(values[i], searchValue, dataType) < 0)
                    continue;
                else
                    break;
            }
            return values[i];
        } else {

            int mid = (end - start) / 2 + start;
            if (values[mid].equals(searchValue))
                return values[mid];

            if (WhereConditionProcessor.compare(values[mid], searchValue, dataType) < 0)
                return binarySearch(values, searchValue, mid + 1, end, dataType);
            else
                return binarySearch(values, searchValue, start, mid - 1, dataType);

        }

    }


}