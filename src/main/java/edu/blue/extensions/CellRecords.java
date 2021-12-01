package edu.blue.extensions;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Denotes each cell in table. Sees value and corresponding value
 *
 * @author Team Blue
 */
public class CellRecords {

    private static final Logger LOGGER = Logger.getLogger(CellRecords.class.getName());

    public byte[] fieldValuebyte;
    public Byte[] fieldValueByte;

    public DBSupportedDataType dataType;
    public String fieldValue;

    CellRecords(DBSupportedDataType dataType, byte[] fieldValue) {
        this.dataType = dataType;
        this.fieldValuebyte = fieldValue;
        try {
            switch (dataType) {
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
                    int hours = seconds / 3600;
                    int remHourSeconds = seconds % 3600;
                    int minutes = remHourSeconds / 60;
                    int remSeconds = remHourSeconds % 60;
                    this.fieldValue = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
                    break;
                case DATETIME:
                    Date rawdatetime = new Date(Long.valueOf(DBDatatypeConversionHelper.longFromByteArray(fieldValuebyte)));
                    this.fieldValue = String.format("%02d", rawdatetime.getYear() + 1900) + "-" + String.format("%02d", rawdatetime.getMonth() + 1)
                            + "-" + String.format("%02d", rawdatetime.getDate()) + "_" + String.format("%02d", rawdatetime.getHours()) + ":"
                            + String.format("%02d", rawdatetime.getMinutes()) + ":" + String.format("%02d", rawdatetime.getSeconds());
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
            LOGGER.log(Level.SEVERE, "Exception while formatting", ex);
        }

    }

    /**
     * Helper for Attributes
     *
     * @param dataType
     * @param fieldValue
     * @throws Exception
     */
    public CellRecords(DBSupportedDataType dataType, String fieldValue) throws Exception {
        this.dataType = dataType;
        this.fieldValue = fieldValue;

        try {
            switch (dataType) {
                case NULL:
                    this.fieldValuebyte = null;
                    break;
                case TINYINT:
                    this.fieldValuebyte = new byte[]{Byte.parseByte(fieldValue)};
                    break;
                case SMALLINT:
                    this.fieldValuebyte = DBDatatypeConversionHelper.shortTobytes(Short.parseShort(fieldValue));
                    break;
                case INT:
                case TIME:
                    this.fieldValuebyte = DBDatatypeConversionHelper.intTobytes(Integer.parseInt(fieldValue));
                    break;
                case BIGINT:
                    this.fieldValuebyte = DBDatatypeConversionHelper.longTobytes(Long.parseLong(fieldValue));
                    break;
                case FLOAT:
                    this.fieldValuebyte = DBDatatypeConversionHelper.floatTobytes(Float.parseFloat(fieldValue));
                    break;
                case DOUBLE:
                    this.fieldValuebyte = DBDatatypeConversionHelper.doubleTobytes(Double.parseDouble(fieldValue));
                    break;
                case YEAR:
                    this.fieldValuebyte = new byte[]{(byte) (Integer.parseInt(fieldValue) - 2000)};
                    break;
                case DATETIME:
                    SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                    Date datetime = sdftime.parse(fieldValue);
                    this.fieldValuebyte = DBDatatypeConversionHelper.longTobytes(datetime.getTime());
                    break;
                case DATE:
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = sdf.parse(fieldValue);
                    this.fieldValuebyte = DBDatatypeConversionHelper.longTobytes(date.getTime());
                    break;
                case TEXT:
                    this.fieldValuebyte = fieldValue.getBytes();
                    break;
                default:
                    this.fieldValuebyte = fieldValue.getBytes(StandardCharsets.US_ASCII);
                    break;
            }
            this.fieldValueByte = DBDatatypeConversionHelper.byteToBytes(fieldValuebyte);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception converting " + fieldValue + " to " + dataType);
            throw e;
        }
    }

}