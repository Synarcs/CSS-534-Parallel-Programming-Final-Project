package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.Agent;
import edu.uw.bothell.css.dsl.MASS.Agents;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Place;
import edu.uw.bothell.css.dsl.MASS.Places;

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

import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;


@SuppressWarnings("unused")
public class Main {

    public Object[] getObjectArrayArgs() {
        return new Object[1];
    }

    private static String[][] readBinaryInput(String filename, int gridSize, int facilityCount){
        Map<String, List<String>> orderMap = new LinkedHashMap<>();

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

    private static final String NODE_FILE = "nodes.xml";
    private static final String INPUT_FILE = "input";

    public static void main(String[] args) throws IOException {

        int facilityCount = Integer.parseInt(args[0]); // includes both fav and unfav

        int favCount = (int) facilityCount / 2; int unFavCount = (int) facilityCount / 2;

        /*
         *  We assume the code  will receive even facilities for binary projections
         * and have equal distribution to fav and unfav
         */

        int gridX = Integer.parseInt(args[1]);
        int gridY = Integer.parseInt(args[2]);


        String[][] tokens = readBinaryInput(INPUT_FILE, gridY, facilityCount);
        System.out.println(Arrays.deepToString(tokens));


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
        spatialGrid.callAll(11, tokens);

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
            // for (Object oo: in){
            // System.out.println((double) oo);
            // }
        }

        String[][] vs = new String[][]{{"a", "b"} , {"c", "d"}, {"e" , "f"}};
        spatialGrid.callAll(4, vs);

        MASS.finish();
    }
}