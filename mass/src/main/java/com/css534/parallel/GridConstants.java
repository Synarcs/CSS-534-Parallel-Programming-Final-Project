package com.css534.parallel;

import java.io.Serializable;

public class GridConstants implements Serializable {
    public static final int INIT = 0;
    public static final int INIT_BINARY_MATRIX = 1;
    public static final int COMPUTE_BEST_ROW_DISTANCE = 2;
    public static final int COMPUTE_PROXIMITY_POLYGONS = 3;;
    public static final int COMPUTE_GLOBAL_SKYLINE = 4;

    public static final String FAVOURABLE = "FAVOURABLE";
    public static final String UNFAVOURABLE = "UNFAVOURABLE";
}
