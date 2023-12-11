package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.Log4J2Logger;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.css534.parallel.GridConstants.INIT;
import static com.css534.parallel.GridConstants.INIT_BINARY_MATRIX;
import static com.css534.parallel.GridConstants.COMPUTE_BEST_ROW_DISTANCE;
import static com.css534.parallel.GridConstants.COMPUTE_PROXIMITY_POLYGONS;
import static com.css534.parallel.GridConstants.COMPUTE_GLOBAL_SKYLINE;

@SuppressWarnings("unused")
public class QuickStart {

    private static final String NODE_FILE = "nodes.xml";
    private static final String INPUT_FILE = "input";

    public Object[] getObjectArrayArgs() {
        return new Object[1];
    }

    private static Log4J2Logger getLogger() { return MASS.getLogger(); }

    private static synchronized String[][] readBinaryInput(String filename, int gridSize, int facilityCount){
        Map<String, List<String>> orderMap = new LinkedHashMap<>();
        getLogger().debug("Reading the binary Grid Matrix");

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] tookens = line.split("\\s+");
                if (tookens.length != 3){}
                String facility = tookens[0];
                int row = Integer.parseInt(tookens[1]);
                String binMatrix = tookens[2];

                if (orderMap.containsKey(facility)){
                    orderMap.get(facility).add(binMatrix);;
                }else{
                    orderMap.put(facility, new ArrayList<>());
                    orderMap.get(facility).add(binMatrix);
                }
            }

            String[][] flattened = new String[facilityCount][gridSize];
            int r = 0;
            for (String facilityType : orderMap.keySet()){
                String[] tokens = new String[gridSize];
                for (int i=0; i < gridSize; i++) tokens[i] = orderMap.get(facilityType).get(i);
                flattened[r] = tokens;
                r++;
            }
            return flattened;
        }catch(IOException exception){
            exception.printStackTrace();
        }

        return null;
    }


    public static void main(String[] args) throws IOException {

        if (args.length != 5){
            System.out.println("the format for the args are <Facility-Count> GridX GridY FavCount UnFavCount");
            System.exit(1);
        }

        int facilityCount = Integer.parseInt(args[0]); // includes both fav and unfav


        /*
         *  We assume the code  will receive even facilities for binary projections
         * and have equal distribution to fav and unfav
         */

        long startTime = System.currentTimeMillis();
        int gridX = Integer.parseInt(args[1]);
        int gridY = Integer.parseInt(args[2]);
        int favCount = Integer.parseInt(args[3]);
        int unFavCount = Integer.parseInt(args[4]);;


        String[][] tokens = readBinaryInput(INPUT_FILE, gridY, facilityCount);
        // System.out.println(Arrays.deepToString(tokens));


        MASS.setNodeFilePath( NODE_FILE );
        MASS.setLoggingLevel( LogLevel.DEBUG );
        MASS.getLogger().debug("Initializing MASS");
        MASS.init();
        MASS.getLogger().debug("Successfully Initialized MASS");
//
//
        MASS.getLogger().debug("Initiating the places Grid across the grid");
        Places spatialGrid = new Places(1, SkylineGridPlaces.class.getName(), (Object) 0, facilityCount);


        int[] fx = new int[]{gridX , gridY};
        spatialGrid.callAll(INIT, fx);
        spatialGrid.callAll(INIT_BINARY_MATRIX, tokens);
        spatialGrid.callAll(COMPUTE_BEST_ROW_DISTANCE);
        spatialGrid.callAll(COMPUTE_PROXIMITY_POLYGONS);

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
        List<SkylineObject> objects = new ArrayList<>();

        // processing all the valid frame index
        for (int i=0; i <  finalFilteredDistancesIndex.length; i++){
            int loopVal = i;
            //System.out.println("the length for this called" + arrayOfObjects.length + " " + arrayOfObjects[0].length);
            Object[] data = spatialGrid.callAll(COMPUTE_GLOBAL_SKYLINE,
                    IntStream.range(0, facilityCount)
                            .mapToObj(row -> Arrays.copyOf(finalFilteredDistancesIndex[loopVal], finalFilteredDistancesIndex[loopVal].length))
                            .map(row -> Arrays.stream(row).boxed().toArray())
                            .toArray(Object[][]::new));
            if (data == null){
                getLogger().debug("Error the output from the call for the index " +
                        finalFilteredDistancesIndex[i][0] + " " + finalFilteredDistancesIndex[i][1]);
            }else {
                double[] doubleDistanceProjection = Arrays.stream(data)
                        .mapToDouble(obj -> (double) obj)
                        .toArray();

                // System.out.println("The distance collected" + Arrays.toString(doubleDistanceProjection));
                boolean isSkyline = distanceAlgorithm.processMinMaxDistanceAlgorithm(doubleDistanceProjection, favCount, unFavCount, finalFilteredDistancesIndex[i][0],
                        finalFilteredDistancesIndex[i][1]);

                if (isSkyline){
                    objects.add(
                            new SkylineObject(finalFilteredDistancesIndex[i][0] + 1, finalFilteredDistancesIndex[i][1] + 1) // 1 based index for row / col projection
                    );
                }
            }

        }
        getLogger().debug("The Skyline objects are:");
        for (SkylineObject object: objects){
            getLogger().debug((object.xx) + " " + object.yy);
        }

        getLogger().debug("Total Processing time " + (System.currentTimeMillis() - startTime));
        MASS.finish();
    }
}