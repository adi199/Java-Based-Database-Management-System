package edu.ottawa.extensions.helpers;

import edu.ottawa.extensions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ottawa.Utils.*;

//Update operations helper for table records
public class UpdateOperationHelper {

    private static final Logger LOGGER = Logger.getLogger(UpdateOperationHelper.class.getName());

    // performs update operation
    public static void updateOperator(String updateString) {
        ArrayList<String> updateTkn = new ArrayList<>(Arrays.asList(updateString.split(" ")));

        String tableName = updateTkn.get(1);
        List<String> updateColumns = new ArrayList<>();
        List<String> valueUpdate = new ArrayList<>();

        if (!updateTkn.get(2).equals("set") || !updateTkn.contains("=")) {
            System.out.println("Invalid Syntax!");

            return;
        }

        String updateColumnInformationStr = updateString.split("set")[1].split("where")[0];

        List<String> newColSet = Arrays.asList(updateColumnInformationStr.split(","));

        try {
            for (String item : newColSet) {
                updateColumns.add(item.split("=")[0].trim());
                valueUpdate.add(item.split("=")[1].trim().replace("\"", "").replace("'", ""));
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Update failed maybe an Invalid Syntax", e);

            return;
        }


        TableInfoHandler meta = new TableInfoHandler(tableName);

        if (!meta.tableExists) {
            System.out.println("Invalid Table name!");
            return;
        }

        if (!meta.columnExists(updateColumns)) {
            System.out.println("!Invalid column name(s)!");
            return;
        }

        WhereConditionProcessor cndtn;
        try {

            cndtn = fetchCndtFrmQuery(meta, updateString);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Update operation failed", e);
            return;

        }


        try {
            RandomAccessFile File = new RandomAccessFile(getTableFilePath(tableName), "rw");
            DBOperationsProcessor bnryFile = new DBOperationsProcessor(File);
            int noOfRecordsupdated = bnryFile.updateRecords(meta, cndtn, updateColumns, valueUpdate);

            if (noOfRecordsupdated > 0) {
                List<Integer> rowIds = new ArrayList<>();
                for (ColumnValueConstrain colInformation : meta.columnNameAttrList) {
                    for (int i = 0; i < updateColumns.size(); i++)
                        if (colInformation.colname.equals(updateColumns.get(i)) && colInformation.hasIdx) {

                            if (cndtn == null) {
                                File f = new File(getIndexFilePath(tableName, colInformation.colname));
                                if (f.exists()) {
                                    f.delete();
                                }

                                if (rowIds.size() == 0) {
                                    BPlusTree bPlusTree1 = new BPlusTree(File, meta.rootPageNumber, meta.tableName);
                                    for (int pgNumber : bPlusTree1.getLeafNodes()) {
                                        Page crntPage = new Page(File, pgNumber);
                                        for (DataRecordForTable dataRecord : crntPage.getPageRecords()) {
                                            rowIds.add(dataRecord.rId);
                                        }
                                    }
                                }
                                RandomAccessFile idxFile = new RandomAccessFile(getIndexFilePath(tableName, updateColumns.get(i)),
                                        "rw");
                                Page.addNewPage(idxFile, PageNodeType.LEAF_INDEX, -1, -1);
                                BTree bT = new BTree(idxFile);
                                bT.insrt(new CellRecords(colInformation.dataType, valueUpdate.get(i)), rowIds);
                            }
                        }
                }
            }

            File.close();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception updating in table " + tableName, e);

        }
    }
}
