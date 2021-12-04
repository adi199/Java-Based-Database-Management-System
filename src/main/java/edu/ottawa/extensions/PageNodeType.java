package edu.ottawa.extensions;

import java.util.HashMap;
import java.util.Map;

/**
 * Holder enum to define type of Page working with
 *
 * @author Team Blue
 */
public enum PageNodeType {
    INTERIOR_TYPE((byte) 5),
    INTERIOR_INDEX((byte) 2),
    LEAF_TYPE((byte) 13),
    LEAF_INDEX((byte) 10);

    private static final Map<Byte, PageNodeType> PAGE_TYPE_LOOKUP_MAP = new HashMap<>();
    private final byte byteValue;

    static {
        for (PageNodeType s : PageNodeType.values())
            PAGE_TYPE_LOOKUP_MAP.put(s.getByteValue(), s);
    }

    PageNodeType(byte value) {
        this.byteValue = value;
    }

    public byte getByteValue() {
        return byteValue;
    }

    public static PageNodeType get(byte value) {
        return PAGE_TYPE_LOOKUP_MAP.get(value);
    }

}