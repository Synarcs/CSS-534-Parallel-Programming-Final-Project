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
public class Skyline {

    private static final String NODE_FILE = "nodes.xml";
    private static final String INPUT_FILE = "input";

    private static Log4J2Logger getLogger() { return MASS.getLogger(); }

    /**
     *  This runs in a synchronized way read only by one thread before doing mass.INIT
     * @param filename
     * @param gridSize
     * @param facilityCount
     * @return the read input file list<List<String>> for the given binary input matrix
     */
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


        int[] fx = new int[]{gridX , gridY};// create the argument for call to set grid dimension internal in each place
        spatialGrid.callAll(INIT, fx); // use call all to configure all places
        spatialGrid.callAll(INIT_BINARY_MATRIX, tokens); // parallely to fill the binary matrix for each pplace
        spatialGrid.callAll(COMPUTE_BEST_ROW_DISTANCE);// to compute row wise distance parallely for each grid in the place
        spatialGrid.callAll(COMPUTE_PROXIMITY_POLYGONS); // run the dominanance relation algorithm parallelly for all places.

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
            // calls all the places to provide the best distance at a given index
            // the length of object array is same as the total places created
            // we pass call all an int flattened array that is provideing call all to all places with specific index
            // So basically to call all places to get index 1,1 location we do places.callAlL(COMPUTE_GLOBAL_SKYLINE, [[1,1],[1,1]...]n (place count).
            Object[] data = spatialGrid.callAll(COMPUTE_GLOBAL_SKYLINE,  // global skyline computation call all with specified index across places / facilities
                    IntStream.range(0, facilityCount)
                            .mapToObj(row -> Arrays.copyOf(finalFilteredDistancesIndex[loopVal], finalFilteredDistancesIndex[loopVal].length))
                            .map(row -> Arrays.stream(row).boxed().toArray())
                            .toArray(Object[][]::new));
            if (data == null){
                getLogger().debug("Error fuck the output from the call for the index " +
                        finalFilteredDistancesIndex[i][0] + " " + finalFilteredDistancesIndex[i][1]);
            }else {
                double[] doubleDistanceProjection = Arrays.stream(data)
                        .mapToDouble(obj -> (double) obj)
                        .toArray();

                // System.out.println("The distance collected" + Arrays.toString(doubleDistanceProjection));
                boolean isSkyline = distanceAlgorithm.processMinMaxDistanceAlgorithm(doubleDistanceProjection, favCount, unFavCount, finalFilteredDistancesIndex[i][0],
                        finalFilteredDistancesIndex[i][1]);
                // finally if a object is found as skyline we store it and log it as the final skyline object.
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