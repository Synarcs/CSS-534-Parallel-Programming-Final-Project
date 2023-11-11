package io.parallel.lbz.sequential;

import java.util.Arrays;
import java.util.Collections;

public class Consts {
    static final int ITERATIONS = 20_000;
    static final int CYLINDER_X_WIDTH = 500;
    static final int CYLINDER_Y_HEIGHT = 100; // MAINTIAN 1 / 5RATION OF THE WIDTH OF INFLOW

    static final int COLLUSION_COM_X = (int) CYLINDER_X_WIDTH / 5;
    static final int COLLUSION_COM_Y = (int) CYLINDER_Y_HEIGHT / 2;

    static final double MAX_HORIZONTAL_INFLOW_VELOCITY = 0.04;

    static boolean DEBUG = false;

    /*
            Simulate the entire grid over the size of 2 dimension with 9 discrete velocities
            Assume 2 macrosocopic quantities as per naive stokes for calculating distances
            Holds the discrete microscopic velocities for each point in the grid.
            DISCRETE VECLOCITY REPRESENTATION ON MICROSCOPIC SCALE
                LBM Grid: D2Q9
                    6   2   5
                      \ | /
                    3 - 0 - 1
                      / | \
                    7   4   8
     */
    static final int[][] LATTICE_VELOCITIES = {
            {0, 1,  0 , -1, 0, 1, -1, -1, 1},
            {0, 0 , 1 ,  0, 1, 1,  1, -1, 1}
    };

    static final int[] LATTICE_INDICES = {0, 1, 2, 3, 4, 5, 6, 7, 8};

    static final int[] OPPOSITE_LATTICE_INDICES = {8,7,6,5,4,3,2,1,0};

    static final int[] LATTICE_WEIGHTS = {
            4/9,                        // Center Velocity [0,]
            1/9,  1/9,  1/9,  1/9,      // Axis-Aligned Velocities [1, 2, 3, 4]
            1/36, 1/36, 1/36, 1/36,     // 45 ° Velocities [5, 6, 7, 8] all the mid slice for the cartesian quadrants
    };

    static int[] PARTICLE_RIGHT_VELOCITIES = {1, 5, 8};

    static int[] PARTICLE_UP_VELOCITIES = {2, 5, 6};
    static int[] PARTICLE_DOWN_VELOCITIES = {4, 7, 8};

    static int[] PARTICLE_LEFT_VELOCITIES = {3, 6, 7};

    static int[] PURE_VERTICAL_VELOCITIES = {0 ,2, 4};

    static int[] PURE_HORIZONTAL_VELOCITIES = {0, 1, 3};


    static final int gridSim = 2;
    static final int N_DISCRETE_VELOCITIES = 9;
    static final int N_MACRO_VELOCITIES = 2;
}
