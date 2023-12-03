package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.*;
import static com.css534.parallel.GridConstants.INIT;

public class Main {
    public static void main(String[] args) {
        int facilityCount = Integer.parseInt(args[0]);
        int gridX = Integer.parseInt(args[1]);
        int gridY = Integer.parseInt(args[2]);

        MASS.getLogger().debug("Initializing MASS");
        MASS.init();
        MASS.getLogger().debug("Successfully Initialized MASS");

        MASS.getLogger().debug("Initiating the places Grid across the grid");
        Places spatialGrid = new Places(1, SkylineGridPlaces.class.getName(), (Object) new Integer(0),
                            gridX, gridY, facilityCount);

        MASS.getLogger().debug("Initiating all the grid placess with binary matrix value");
        Object[] globalBinaryGrid = new Object[gridY * gridY * facilityCount];;

        spatialGrid.callAll(INIT, globalBinaryGrid);
        MASS.getLogger().debug("Initialization done for the skyline object grid");


        int totalSpatialgridSize = spatialGrid.getPlacesSize();

        Place[] distributedArray = spatialGrid.getPlaces();


        for (int i=0; i < facilityCount; i++){

        }

        MASS.finish();
    }
}