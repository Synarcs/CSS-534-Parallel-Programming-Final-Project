package com.css534.parallel;

import java.io.Serializable;

public class DelimeterRegexConsts implements Serializable {
    public final static String SKYLINE_OBJECTS_LOADER = "^(.*?)\\s*\\|\\|.*$";
    public final static String FACILITY_TYPE = ".*-$";

    public final static Integer FAVOURABLE_POSITION = 1;

    public final static Integer UNFAVOURABLE_POSITION = 0;
    public final static int FACILITY_COUNT = 2;
}
