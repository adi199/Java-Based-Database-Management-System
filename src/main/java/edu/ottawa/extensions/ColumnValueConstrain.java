package edu.ottawa.extensions;

import edu.ottawa.Utils;

import java.io.File;
import java.util.logging.Logger;

// This class helps identify column and data type with respect to metadata of table
public class ColumnValueConstrain {

    private static final Logger LOGGER = Logger.getLogger(ColumnValueConstrain.class.getName());

    public DBSupportedDataType dataType;
    public String colname;
    public boolean isSame;
    public boolean isNull;
    public Short ordPosition;
    public boolean hasIdx;
    public String tblName;
    public boolean isPk;

    public ColumnValueConstrain() {

    }

    ColumnValueConstrain(String tblName, DBSupportedDataType dataType, String colname, boolean isSame, boolean isNull, short ordPosition) {
        this.dataType = dataType;
        this.colname = colname;
        this.isSame = isSame;
        this.isNull = isNull;
        this.ordPosition = ordPosition;
        this.tblName = tblName;

        this.hasIdx = (new File(Utils.getIndexFilePath(tblName, colname)).exists());

    }


    public void setAsPrimaryKey() {
        isPk = true;
    }
}