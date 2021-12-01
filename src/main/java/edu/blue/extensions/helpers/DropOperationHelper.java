package edu.blue.extensions.helpers;

import edu.blue.Utils;
import edu.blue.extensions.DBOperationsProcessor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.blue.extensions.helpers.DeleteOperationHelper.performDeleteOperation;

/**
 * This helper facade is used for performing drop operation on the table and implicitly drops the indexes if
 * any of them are specified on the tables columns
 *
 * @author Team Blue
 */
public class DropOperationHelper {

    private static final Logger LOGGER = Logger.getLogger(DropOperationHelper.class.getName());

    /**
     * This method performs the record deletion operation.
     *
     * @param dropTableString
     */
    public static void dropTable(String dropTableString) {
        try {
            String[] tokens = dropTableString.split(" ");
            if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
                System.out.println("Error");
                return;
            }

            ArrayList<String> dropTableTokens = new ArrayList<>(Arrays.asList(dropTableString.split(" ")));
            String tableName = dropTableTokens.get(2);


            performDeleteOperation("delete from table " + DBOperationsProcessor.tablesTable + " where table_name = '" + tableName + "' ");
            performDeleteOperation("delete from table " + DBOperationsProcessor.columnsTable + " where table_name = '" + tableName + "' ");
            File tableFile = new File(Utils.getTableFilePath(tableName));
            if (tableFile.delete()) {
                System.out.println("Table " + tableName + " dropped successfully.");
            } else System.out.println("Table doesn't exist!");


            File f = new File("data/user_data/");
            File[] matchingFiles = f.listFiles((dir, name) -> name.startsWith(tableName) && name.endsWith("ndx"));
            boolean flag = false;
            for (File file : matchingFiles) {
                if (file.delete()) {
                    flag = true;
                }
            }
            if (flag)
                System.out.println("Dropped indexes");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed dropping table ", e);
        }
    }

}
