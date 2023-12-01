package com.css534.parallel;

import java.io.Serializable;

public class DelimeterRegexConsts implements Serializable {
    public final static String SKYLINE_OBJECTS_LOADER = "^(.*?)\\s*\\|\\|.*$";
    public final static String FACILITY_TYPE = ".*-$";

    public final static String FAVOURABLE_POSITION = "FAVOURABLE";

    public final static String UNFAVOURABLE_POSITION = "UNFAVOURABLE";
    public final static int FACILITY_COUNT = 2;
}
