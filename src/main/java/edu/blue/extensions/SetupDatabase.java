package edu.blue.extensions;

import java.io.File;

/**
 * This helper class is called initially to setup the database if its not already setup
 *
 * @author Team Blue
 */
public class SetupDatabase {
    public static void initDB() {
        File baseDBFileDirectory = new File("data");

        if (!new File(baseDBFileDirectory, DBOperationsProcessor.tablesTable + ".tbl").exists()
                || !new File(baseDBFileDirectory, DBOperationsProcessor.columnsTable + ".tbl").exists())
            DBOperationsProcessor.initializeDataBaseStore();
        else
            DBOperationsProcessor.dataStoreInitialized = true;

    }
}
