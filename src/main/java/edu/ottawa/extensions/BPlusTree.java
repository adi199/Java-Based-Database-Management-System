package edu.ottawa.extensions;

import edu.ottawa.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.logging.Logger;

//BPlus tree implementation to traverse files in DB
public class BPlusTree {

    private static final Logger lgr = Logger.getLogger(BPlusTree.class.getName());

    RandomAccessFile bnryFile;
    int pageNumberOfRootpage;
    String tableName;

    public BPlusTree(RandomAccessFile file, int pageNumberOfRootpage, String tableName) {
        this.bnryFile = file;
        this.pageNumberOfRootpage = pageNumberOfRootpage;
        this.tableName = tableName;
    }

    // returns list of leaf nodes
    public List<Integer> getLeafNodes() throws IOException {

        List<Integer> lfPage = new ArrayList<>();
        bnryFile.seek(pageNumberOfRootpage * DBOperationsProcessor.pageSize);
        PageNodeType rootPgTYp = PageNodeType.get(bnryFile.readByte());
        if (rootPgTYp == PageNodeType.LEAF_TYPE) {
            if (!lfPage.contains(pageNumberOfRootpage))
                lfPage.add(pageNumberOfRootpage);
        } else {
            insertLeafNode(pageNumberOfRootpage, lfPage);
        }

        return lfPage;

    }

    //Adds a leaf node
    private void insertLeafNode(int innerPageNum, List<Integer> lfPage) throws IOException {
        Page innerPg = new Page(bnryFile, innerPageNum);
        for (InternalTableRecord lftPage : innerPg.lChild) {
            if (Page.getPageType(bnryFile, lftPage.leftPageNumber) == PageNodeType.LEAF_TYPE) {
                if (!lfPage.contains(lftPage.leftPageNumber))
                    lfPage.add(lftPage.leftPageNumber);
            } else {
                insertLeafNode(lftPage.leftPageNumber, lfPage);
            }
        }

        if (Page.getPageType(bnryFile, innerPg.rPage) == PageNodeType.LEAF_TYPE) {
            if (!lfPage.contains(innerPg.rPage))
                lfPage.add(innerPg.rPage);
        } else {
            insertLeafNode(innerPg.rPage, lfPage);
        }

    }

    //returns leave nodes
    public List<Integer> getLeafNodes(WhereConditionProcessor cndtn) throws IOException {

        if (cndtn == null || cndtn.getOperation() == SupportedOperators.NOT_EQUAL
                || !(new File(Utils.getIndexFilePath(tableName, cndtn.columnName)).exists())) {
            /*as there is no index  brute force  */

            return getLeafNodes();
        } else {

            RandomAccessFile idxFile = new RandomAccessFile(
                    Utils.getIndexFilePath(tableName, cndtn.columnName), "r");
            BTree bTree = new BTree(idxFile);

            /* Binary search  the tree*/
            List<Integer> idRow = bTree.fetchRowId(cndtn);
            Set<Integer> hashSet = new HashSet<>();

            for (int rowId : idRow) {
                hashSet.add(getPgNbr(rowId, new Page(bnryFile, pageNumberOfRootpage)));
            }

            idxFile.close();

            return Arrays.asList(hashSet.toArray(new Integer[hashSet.size()]));
        }

    }

    //Returns the right most child
    public static int fetchPgNbrToInsert(RandomAccessFile file, int rtPgnumber) {
        Page rootPage = new Page(file, rtPgnumber);
        if (rootPage.pageType != PageNodeType.LEAF_TYPE && rootPage.pageType != PageNodeType.LEAF_INDEX)
            return fetchPgNbrToInsert(file, rootPage.rPage);
        else
            return rtPgnumber;

    }

    // return specified page
    public int getPgNbr(int rId, Page pg) {
        if (pg.pageType == PageNodeType.LEAF_TYPE)
            return pg.pageNumber;

        int idx = bnrySrch(pg.lChild, rId, 0, pg.noOfCells - 1);

        if (rId < pg.lChild.get(idx).rowId) {
            return getPgNbr(rId, new Page(bnryFile, pg.lChild.get(idx).leftPageNumber));
        } else {
            if (idx + 1 < pg.lChild.size())
                return getPgNbr(rId, new Page(bnryFile, pg.lChild.get(idx + 1).leftPageNumber));
            else
                return getPgNbr(rId, new Page(bnryFile, pg.rPage));


        }
    }

    // performs binary search
    private int bnrySrch(List<InternalTableRecord> val, int srchVal, int start, int end) {

        if (end - start <= 2) {
            int i;
            for (i = start; i < end; i++) {
                if (val.get(i).rowId < srchVal)
                    continue;
                else
                    break;
            }
            return i;
        } else {

            int mid = (end - start) / 2 + start;
            if (val.get(mid).rowId == srchVal)
                return mid;

            if (val.get(mid).rowId < srchVal)
                return bnrySrch(val, srchVal, mid + 1, end);
            else
                return bnrySrch(val, srchVal, start, mid - 1);

        }

    }

}