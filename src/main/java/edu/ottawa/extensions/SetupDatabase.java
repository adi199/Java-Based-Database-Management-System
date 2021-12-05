package edu.ottawa.extensions;

import java.io.File;

/**
 * This class is called initially to set up the database if it's not already setup
 *
 * @author Team Ottawa
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
