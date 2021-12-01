package edu.blue.extensions;

import edu.blue.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.logging.Logger;

/**
 * BPlus tree implementation for traversing the Table files in the DB
 *
 * @author Team Blue
 */
public class BPlusTree {

    private static final Logger LOGGER = Logger.getLogger(BPlusTree.class.getName());

    RandomAccessFile binaryFile;
    int rootpageNumber;
    String tableName;

    public BPlusTree(RandomAccessFile file, int rootpageNumber, String tableName) {
        this.binaryFile = file;
        this.rootpageNumber = rootpageNumber;
        this.tableName = tableName;
    }


    /**
     * Gets all leaf nodes
     *
     * @return
     * @throws IOException
     */
    public List<Integer> getAllLeaveNodes() throws IOException {

        List<Integer> leafPages = new ArrayList<>();
        binaryFile.seek(rootpageNumber * DBOperationsProcessor.pageSize);
        /*
         *if root is leaf page no traversal required
         */
        PageNodeType rootPageType = PageNodeType.get(binaryFile.readByte());
        if (rootPageType == PageNodeType.LEAF_TYPE) {
            if (!leafPages.contains(rootpageNumber))
                leafPages.add(rootpageNumber);
        } else {
            addLeaveNode(rootpageNumber, leafPages);
        }

        return leafPages;

    }

    /**
     * adds leave nodes
     */
    private void addLeaveNode(int insidepageNumber, List<Integer> leafPages) throws IOException {
        Page insidePage = new Page(binaryFile, insidepageNumber);
        for (InternalTableRecord leftPage : insidePage.lChild) {
            if (Page.getPageType(binaryFile, leftPage.leftPageNumber) == PageNodeType.LEAF_TYPE) {
                if (!leafPages.contains(leftPage.leftPageNumber))
                    leafPages.add(leftPage.leftPageNumber);
            } else {
                addLeaveNode(leftPage.leftPageNumber, leafPages);
            }
        }

        if (Page.getPageType(binaryFile, insidePage.rPage) == PageNodeType.LEAF_TYPE) {
            if (!leafPages.contains(insidePage.rPage))
                leafPages.add(insidePage.rPage);
        } else {
            addLeaveNode(insidePage.rPage, leafPages);
        }

    }

    /**
     * Returns all leave nodes
     *
     * @param condition
     * @return
     * @throws IOException
     */
    public List<Integer> getAllLeaveNodes(WhereConditionProcessor condition) throws IOException {

        if (condition == null || condition.getOperation() == SupportedOperators.NOTEQUAL
                || !(new File(Utils.getIndexFilePath(tableName, condition.columnName)).exists())) {
            /*as there is no index  brute force  */

            return getAllLeaveNodes();
        } else {

            RandomAccessFile indexFile = new RandomAccessFile(
                    Utils.getIndexFilePath(tableName, condition.columnName), "r");
            BTree bTree = new BTree(indexFile);

            /* Binary search  the tree*/
            List<Integer> rowIds = bTree.getRowIds(condition);
            Set<Integer> hash_Set = new HashSet<>();

            for (int rowId : rowIds) {
                hash_Set.add(getPageNumber(rowId, new Page(binaryFile, rootpageNumber)));
            }

            indexFile.close();

            return Arrays.asList(hash_Set.toArray(new Integer[hash_Set.size()]));
        }

    }

    /**
     * Returns the right most child
     */
    public static int getPageNumberForInsertion(RandomAccessFile file, int rootpageNumber) {
        Page rootPage = new Page(file, rootpageNumber);
        if (rootPage.pageType != PageNodeType.LEAF_TYPE && rootPage.pageType != PageNodeType.LEAF_INDEX)
            return getPageNumberForInsertion(file, rootPage.rPage);
        else
            return rootpageNumber;

    }

    /**
     * Obtains the required page
     *
     * @param rowId
     * @param page
     * @return
     */
    public int getPageNumber(int rowId, Page page) {
        if (page.pageType == PageNodeType.LEAF_TYPE)
            return page.pageNumber;

        int index = binarySearch(page.lChild, rowId, 0, page.noOfCells - 1);

        if (rowId < page.lChild.get(index).rowId) {
            return getPageNumber(rowId, new Page(binaryFile, page.lChild.get(index).leftPageNumber));
        } else {
            if (index + 1 < page.lChild.size())
                return getPageNumber(rowId, new Page(binaryFile, page.lChild.get(index + 1).leftPageNumber));
            else
                return getPageNumber(rowId, new Page(binaryFile, page.rPage));


        }
    }

    /**
     * Performs binary search
     *
     * @param values
     * @param searchValue
     * @param start
     * @param last
     * @return
     */
    private int binarySearch(List<InternalTableRecord> values, int searchValue, int start, int last) {

        if (last - start <= 2) {
            int i;
            for (i = start; i < last; i++) {
                if (values.get(i).rowId < searchValue)
                    continue;
                else
                    break;
            }
            return i;
        } else {

            int mid = (last - start) / 2 + start;
            if (values.get(mid).rowId == searchValue)
                return mid;

            if (values.get(mid).rowId < searchValue)
                return binarySearch(values, searchValue, mid + 1, last);
            else
                return binarySearch(values, searchValue, start, mid - 1);

        }

    }

}