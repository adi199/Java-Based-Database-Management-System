package edu.ottawa.extensions.helpers;

import edu.ottawa.Utils;
import edu.ottawa.extensions.DBOperationsProcessor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ottawa.extensions.helpers.DeleteOperationHelper.handleDeleteOperation;

/**
 * This helper facade is used to perform drop operation on the table and implicitly drops the indexes if
 * any of them are specified on the tables columns
 *
 * @author Team Ottawa
 */
public class DropOperationHelper {

    private static final Logger logger = Logger.getLogger(DropOperationHelper.class.getName());

    /**
     * This method used to perform the record deletion operation.
     *
     * @param dropTableQueryString
     */
    public static void handleDropTableOperation(String dropTableQueryString) {
        try {
            String[] tokens = dropTableQueryString.split(" ");
            if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
                System.out.println("Invalid Syntax!");
                return;
            }

            ArrayList<String> dropTableTokens = new ArrayList<>(Arrays.asList(dropTableQueryString.split(" ")));
            String tableName = dropTableTokens.get(2);


            handleDeleteOperation("delete from table " + DBOperationsProcessor.tablesTable + " where table_name = '" + tableName + "' ");
            handleDeleteOperation("delete from table " + DBOperationsProcessor.columnsTable + " where table_name = '" + tableName + "' ");
            File tableFile = new File(Utils.getTableFilePath(tableName));
            if (tableFile.delete()) {
                System.out.println(tableName + " - Table dropped successfully.");
            } else System.out.println("Table doesn't exist!");


            File file = new File("data/user_data/");
            File[] matchingFiles = file.listFiles((dir, name) -> name.startsWith(tableName) && name.endsWith("ndx"));
            boolean deleteFlag = false;
            for (File fileRecord : matchingFiles) {
                if (fileRecord.delete()) {
                    deleteFlag = true;
                }
            }
            if (deleteFlag)
                System.out.println("Indexes dropped successfully");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed dropping table ", e);
        }
    }

}
