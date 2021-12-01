package edu.blue.extensions;

import edu.blue.Utils;

import java.io.File;
import java.util.logging.Logger;

/**
 * Helper Class to denote column name and datatype for table metadata
 *
 * @author Team Blue
 */
public class ColumnValueConstrain {

    private static final Logger LOGGER = Logger.getLogger(ColumnValueConstrain.class.getName());

    public DBSupportedDataType dataType;
    public String columnName;
    public boolean isUnique;
    public boolean isNullable;
    public Short ordinalPosition;
    public boolean hasIndex;
    public String tableName;
    public boolean isPrimaryKey;

    public ColumnValueConstrain() {

    }

    ColumnValueConstrain(String tableName, DBSupportedDataType dataType, String columnName, boolean isUnique, boolean isNullable, short ordinalPosition) {
        this.dataType = dataType;
        this.columnName = columnName;
        this.isUnique = isUnique;
        this.isNullable = isNullable;
        this.ordinalPosition = ordinalPosition;
        this.tableName = tableName;

        this.hasIndex = (new File(Utils.getIndexFilePath(tableName, columnName)).exists());

    }


    public void setAsPrimaryKey() {
        isPrimaryKey = true;
    }
}