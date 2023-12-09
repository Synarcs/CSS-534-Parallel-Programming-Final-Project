package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;


@SuppressWarnings("unused")
public class Main {

    public Object[] getObjectArrayArgs() {
        return new Object[1];
    }

    private static double[][] initializeArray(int numberOfFacilities, int n, String filePath)
            throws IOException {
        double[][] facilityGrid = new double[numberOfFacilities][n];
        int facilityIndex;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = "";
            String[] tokens = line.split("\\s+");
            if (tokens.length != 3){}
            String binaryGrid = tokens[tokens.length - 1];
        }

        return facilityGrid;
    }
    private static final String NODE_FILE = "nodes.xml";

    public static void main(String[] args) throws IOException {

        int facilityCount = Integer.parseInt(args[0]); // includes both fav and unfav

        int favCount = (int) facilityCount / 2; int unFavCount = (int) facilityCount / 2;

        /*
         *  We assume the code  will receive even facilities for binary projections
         * and have equal distribution to fav and unfav
         */

        int gridX = Integer.parseInt(args[1]);
        int gridY = Integer.parseInt(args[2]);


        MASS.setNodeFilePath( NODE_FILE );
        MASS.setLoggingLevel( LogLevel.DEBUG );
        MASS.getLogger().debug("Initializing MASS");
        MASS.init();
        MASS.getLogger().debug("Successfully Initialized MASS");
//
//
        MASS.getLogger().debug("Initiating the places Grid across the grid");
        Places spatialGrid = new Places(1, SkylineGridPlaces.class.getName(), (Object) "input", facilityCount);


        int[] fx = new int[]{gridX , gridY};
        spatialGrid.callAll(0, fx);

        spatialGrid.callAll(1);

        // Agents agent = new Agents(2, SkylineAgent.class.getName(), (Object) 0, spatialGrid ,  facilityCount);

        int[][] finalFilteredDistancesIndex = new int[gridX * gridY][2];

        int projectionIndex = 0; int col = 0;
        for (int i = 0; i < gridX; i++){
            for (int j=0; j < gridX; j++){
                finalFilteredDistancesIndex[projectionIndex][col] = i;
                finalFilteredDistancesIndex[projectionIndex][col + 1] = j;
                projectionIndex++;
            }
        }

        int endCol = finalFilteredDistancesIndex[0].length;
        int startCol = 0;

        // System.out.println(Arrays.deepToString(finalFilteredDistancesIndex));
        MinMaxDistance distanceAlgorithm = new MinMaxDistance();

        int ss = 0; int se = 0;
        // processing all the valid frame index
        for (int i=0; i <  finalFilteredDistancesIndex.length; i++){
            System.out.println("Invoked a function call all");
            int loopVal = i;
            Object[][] arrayOfObjects =  IntStream.range(0, facilityCount)
                    .mapToObj(row -> Arrays.copyOf(finalFilteredDistancesIndex[loopVal], finalFilteredDistancesIndex[loopVal].length))
                    .map(row -> Arrays.stream(row).boxed().toArray())
                    .toArray(Object[][]::new);
            System.out.println("the length for this called" + arrayOfObjects.length + " " + arrayOfObjects[0].length);
            Object[] in  =spatialGrid.callAll(3, arrayOfObjects);

            distanceAlgorithm.processMinMaxDistanceAlgorithm(in, favCount, unFavCount, finalFilteredDistancesIndex[0][0],
                    finalFilteredDistancesIndex[0][1]);

            System.out.println(in.getClass().getName() + " " + in.getClass().getCanonicalName());
            for (Object oo: in){
                System.out.println((double) oo);
            }
        }

        MASS.finish();
    }
}