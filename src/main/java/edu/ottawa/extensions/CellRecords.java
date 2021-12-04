package edu.ottawa.extensions;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

// Class to manipulate cell data for a table.
public class CellRecords {

    private static final Logger lgr = Logger.getLogger(CellRecords.class.getName());

    public byte[] fieldValuebyte;
    public Byte[] fieldValueByte;

    public DBSupportedDataType dataType;
    public String fieldValue;

    CellRecords(DBSupportedDataType dType, byte[] colValue) {
        this.dataType = dType;
        this.fieldValuebyte = colValue;
        try {
            switch (dType) {
                case NULL:
                    this.fieldValue = "NULL";
                    break;
                case TINYINT:
                    this.fieldValue = Byte.valueOf(DBDatatypeConversionHelper.byteFromByteArray(fieldValuebyte)).toString();
                    break;
                case SMALLINT:
                    this.fieldValue = Short.valueOf(DBDatatypeConversionHelper.shortFromByteArray(fieldValuebyte)).toString();
                    break;
                case INT:
                    this.fieldValue = Integer.valueOf(DBDatatypeConversionHelper.intFromByteArray(fieldValuebyte)).toString();
                    break;
                case BIGINT:
                    this.fieldValue = Long.valueOf(DBDatatypeConversionHelper.longFromByteArray(fieldValuebyte)).toString();
                    break;
                case FLOAT:
                    this.fieldValue = Float.valueOf(DBDatatypeConversionHelper.floatFromByteArray(fieldValuebyte)).toString();
                    break;
                case DOUBLE:
                    this.fieldValue = Double.valueOf(DBDatatypeConversionHelper.doubleFromByteArray(fieldValuebyte)).toString();
                    break;
                case YEAR:
                    this.fieldValue = Integer.valueOf((int) Byte.valueOf(DBDatatypeConversionHelper.byteFromByteArray(fieldValuebyte)) + 2000).toString();
                    break;
                case TIME:
                    int millisSinceMidnight = DBDatatypeConversionHelper.intFromByteArray(fieldValuebyte) % 86400000;
                    int seconds = millisSinceMidnight / 1000;
                    int hour = seconds / 3600;
                    int remHourSeconds = seconds % 3600;
                    int minutes = remHourSeconds / 60;
                    int remainingSecs = remHourSeconds % 60;
                    this.fieldValue = String.format("%02d", hour) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remainingSecs);
                    break;
                case DATETIME:
                    Date rawDtTime = new Date(Long.valueOf(DBDatatypeConversionHelper.longFromByteArray(fieldValuebyte)));
                    this.fieldValue = String.format("%02d", rawDtTime.getYear() + 1900) + "-" + String.format("%02d", rawDtTime.getMonth() + 1)
                            + "-" + String.format("%02d", rawDtTime.getDate()) + "_" + String.format("%02d", rawDtTime.getHours()) + ":"
                            + String.format("%02d", rawDtTime.getMinutes()) + ":" + String.format("%02d", rawDtTime.getSeconds());
                    break;
                case DATE:
                    Date rawdate = new Date(Long.valueOf(DBDatatypeConversionHelper.longFromByteArray(fieldValuebyte)));
                    this.fieldValue = String.format("%02d", rawdate.getYear() + 1900) + "-" + String.format("%02d", rawdate.getMonth() + 1)
                            + "-" + String.format("%02d", rawdate.getDate());
                    break;
                case TEXT:
                default:
                    this.fieldValue = new String(fieldValuebyte, StandardCharsets.UTF_8);
                    break;
            }
            this.fieldValueByte = DBDatatypeConversionHelper.byteToBytes(fieldValuebyte);
        } catch (Exception ex) {
            lgr.log(Level.SEVERE, "Exception while formatting", ex);
        }

    }

    // Manages attributes
    public CellRecords(DBSupportedDataType dType, String colVal) throws Exception {
        this.dataType = dType;
        this.fieldValue = colVal;

        try {
            switch (dType) {
                case NULL:
                    this.fieldValuebyte = null;
                    break;
                case TINYINT:
                    this.fieldValuebyte = new byte[]{Byte.parseByte(colVal)};
                    break;
                case SMALLINT:
                    this.fieldValuebyte = DBDatatypeConversionHelper.shortTobytes(Short.parseShort(colVal));
                    break;
                case INT:
                case TIME:
                    this.fieldValuebyte = DBDatatypeConversionHelper.intTobytes(Integer.parseInt(colVal));
                    break;
                case BIGINT:
                    this.fieldValuebyte = DBDatatypeConversionHelper.longTobytes(Long.parseLong(colVal));
                    break;
                case FLOAT:
                    this.fieldValuebyte = DBDatatypeConversionHelper.floatTobytes(Float.parseFloat(colVal));
                    break;
                case DOUBLE:
                    this.fieldValuebyte = DBDatatypeConversionHelper.doubleTobytes(Double.parseDouble(colVal));
                    break;
                case YEAR:
                    this.fieldValuebyte = new byte[]{(byte) (Integer.parseInt(colVal) - 2000)};
                    break;
                case DATETIME:
                    SimpleDateFormat SimpleDtFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                    Date datetime = SimpleDtFormat.parse(colVal);
                    this.fieldValuebyte = DBDatatypeConversionHelper.longTobytes(datetime.getTime());
                    break;
                case DATE:
                    SimpleDateFormat simpleDtFormat = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = simpleDtFormat.parse(colVal);
                    this.fieldValuebyte = DBDatatypeConversionHelper.longTobytes(date.getTime());
                    break;
                case TEXT:
                    this.fieldValuebyte = colVal.getBytes();
                    break;
                default:
                    this.fieldValuebyte = colVal.getBytes(StandardCharsets.US_ASCII);
                    break;
            }
            this.fieldValueByte = DBDatatypeConversionHelper.byteToBytes(fieldValuebyte);
        } catch (Exception e) {
            lgr.log(Level.SEVERE, "Exception converting " + colVal + " to " + dType);
            throw e;
        }
    }

}