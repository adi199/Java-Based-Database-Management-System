package edu.ottawa.extensions;

import java.util.HashMap;
import java.util.Map;


//This helper class helps to represent various data types from database
public enum DBSupportedDataType {
    NULL((byte) 0) {
        @Override
        public String toString() {
            return "NULL";
        }
    },
    TINYINT((byte) 1) {
        @Override
        public String toString() {
            return "TINYINT";
        }
    },
    SMALLINT((byte) 2) {
        @Override
        public String toString() {
            return "SMALLINT";
        }
    },
    INT((byte) 3) {
        @Override
        public String toString() {
            return "INT";
        }
    },
    BIGINT((byte) 4) {
        @Override
        public String toString() {
            return "BIGINT";
        }
    },
    FLOAT((byte) 5) {
        @Override
        public String toString() {
            return "FLOAT";
        }
    },
    DOUBLE((byte) 6) {
        @Override
        public String toString() {
            return "DOUBLE";
        }
    },
    YEAR((byte) 8) {
        @Override
        public String toString() {
            return "YEAR";
        }
    },
    TIME((byte) 9) {
        @Override
        public String toString() {
            return "TIME";
        }
    },
    DATETIME((byte) 10) {
        @Override
        public String toString() {
            return "DATETIME";
        }
    },
    DATE((byte) 11) {
        @Override
        public String toString() {
            return "DATE";
        }
    },
    TEXT((byte) 12) {
        @Override
        public String toString() {
            return "TEXT";
        }
    };


    private static final Map<Byte, DBSupportedDataType> dataTypeLookup = new HashMap<>();
    private static final Map<Byte, Integer> dataTypeSizeLookup = new HashMap<>();
    private static final Map<String, DBSupportedDataType> dataTypeStringLookup = new HashMap<>();
    private static final Map<DBSupportedDataType, Integer> dataTypePrintOffset = new HashMap<>();


    static {
        for (DBSupportedDataType s : DBSupportedDataType.values()) {
            dataTypeLookup.put(s.getValue(), s);
            dataTypeStringLookup.put(s.toString(), s);

            if (s == DBSupportedDataType.TINYINT || s == DBSupportedDataType.YEAR) {
                dataTypeSizeLookup.put(s.getValue(), 1);
                dataTypePrintOffset.put(s, 6);
            } else if (s == DBSupportedDataType.SMALLINT) {
                dataTypeSizeLookup.put(s.getValue(), 2);
                dataTypePrintOffset.put(s, 8);
            } else if (s == DBSupportedDataType.INT || s == DBSupportedDataType.FLOAT || s == DBSupportedDataType.TIME) {
                dataTypeSizeLookup.put(s.getValue(), 4);
                dataTypePrintOffset.put(s, 10);
            } else if (s == DBSupportedDataType.BIGINT || s == DBSupportedDataType.DOUBLE
                    || s == DBSupportedDataType.DATETIME || s == DBSupportedDataType.DATE) {
                dataTypeSizeLookup.put(s.getValue(), 8);
                dataTypePrintOffset.put(s, 25);
            } else if (s == DBSupportedDataType.TEXT) {
                dataTypePrintOffset.put(s, 25);
            } else if (s == DBSupportedDataType.NULL) {
                dataTypeSizeLookup.put(s.getValue(), 0);
                dataTypePrintOffset.put(s, 6);
            }
        }


    }

    private final byte value;

    DBSupportedDataType(byte value) {
        this.value = value;
    }


    public byte getValue() {
        return value;
    }

    public static DBSupportedDataType get(byte value) {
        if (value > 12)
            return DBSupportedDataType.TEXT;
        return dataTypeLookup.get(value);
    }


    //Function to get the datatype from string map to a DB Supported datatype.
    public static DBSupportedDataType get(String text) {
        return dataTypeStringLookup.get(text);
    }

    public static int getLength(byte value) {
        if (get(value) != DBSupportedDataType.TEXT)
            return dataTypeSizeLookup.get(value);
        else
            return value - 12;
    }

    public int getPrintOffset() {
        return dataTypePrintOffset.get(get(this.value));
    }


}