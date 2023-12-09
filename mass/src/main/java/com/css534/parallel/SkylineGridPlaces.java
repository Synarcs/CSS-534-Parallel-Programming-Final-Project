package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Place;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.stream.IntStream;

@SuppressWarnings("unused")
public class SkylineGridPlaces extends Place {
    // file object to be read by all the places file shared across all the computing nodes
    public static FileInputStream file = null;
    public static String filename = null;
    private static boolean debug = false;


    public SkylineGridPlaces(Object object){
        filename = (String) object;
    }
    public SkylineGridPlaces(){
        super();
    }
    private double[][] facilityGridPlaces;
    private double[][] distanceGrid;
    private boolean isFavourableFacility;
    private int facilityType;
    private int facilityCount; int gridX = -1; int gridY = -1;

    private void configurePlaces(Object args){

    }


    private double[] getLeftDistance(double[] gridRows){
        boolean isFavlFound = false;
        double[] leftDistance = new double[gridRows.length];
        Arrays.fill(leftDistance, Double.MAX_VALUE);
        for (int i=0; i < gridRows.length; i++){
            if (gridRows[i] == 1){
                // a facility one
                leftDistance[i] = 0;
                isFavlFound = true;
            }else {
                if (isFavlFound){
                    leftDistance[i] = leftDistance[i-1] + 1;
                }
            }
        }
        return leftDistance;
    }

    private double[] getRightDistance(double[] gridRows){
        boolean isFavrFound = false;
        double[] rightDistance = new double[gridRows.length];
        Arrays.fill(rightDistance, Double.MAX_VALUE);

        for (int i=gridRows.length-1; i >=0; --i){
            if (gridRows[i] == 1){
                rightDistance[i] = 0;
                isFavrFound = true;
            }else {
                if (isFavrFound){
                    rightDistance[i] = rightDistance[i + 1] + 1;
                }
            }
        }
        return rightDistance;
    }

    public Object init(Object facilityName){
        int totalFacilities = getSize()[0];
        int currentFacility = getIndex()[0] + 1;

        int[] gridSizePerFacility = (int[]) facilityName;

        facilityGridPlaces = new double[gridSizePerFacility[0]][gridSizePerFacility[1]];
        gridX = gridSizePerFacility[0];
        gridY = gridSizePerFacility[1];

        System.out.println(facilityGridPlaces.length + " " + facilityGridPlaces[0].length + " " + filename);

        String fileName = "input";
        synchronized (fileName){
            try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                String fileLine;
                LineNumberReader reader = new LineNumberReader(br);
                int lineNumber = reader.getLineNumber();

                while ((fileLine = br.readLine()) != null) {
                    String[] val = fileLine.split("\\s+");
                    if (val.length == 0 || val[0].indexOf("F") == -1)
                        throw new RuntimeException("The required file of not correct format for parsing");

                    if (Integer.valueOf(val[0].indexOf("-") == -1 ? val[0].replace("F", "") : String.valueOf(val[0].charAt(1))) == currentFacility){
                        String binaryIndex = val[val.length - 1];

                        for (int i= 0; i < binaryIndex.length();i ++){
                            facilityGridPlaces[lineNumber % gridSizePerFacility[0]][i] = Double.valueOf(
                                    Character.getNumericValue(binaryIndex.charAt(i))
                            );
                            if (debug)
                                System.out.println("For the index " + currentFacility + "value is " + binaryIndex + "distance is " + facilityGridPlaces[lineNumber % gridSizePerFacility[0]][i]);
                        }
                    }

                }
            }catch(IOException exception){
                exception.printStackTrace();
            }catch(RuntimeException exception){ exception.printStackTrace(); }
        }
        return null;
    }

    public void computeBestAgentDistance(){
        if (gridX != -1 && gridY != -1){}

        int distanceRowIndex = 0;
        distanceGrid = new double[gridX][gridY];

        for (double[] row: facilityGridPlaces){
            double[] leftDistance = getLeftDistance(row);
            double[] rightDistance = getRightDistance(row);

            for (int k=0; k < gridY; k++){
                distanceGrid[distanceRowIndex][k] = Double.min(
                        leftDistance[k], rightDistance[k]
                );
            }
            distanceRowIndex++;
        }
    }

    private void computeProximityPolygons(Object argument){

    }

    public double test(Object argument){

        System.out.println(argument.getClass().getName() + argument.getClass().getCanonicalName());
        Object[] indexAskedByAgent = (Object[]) argument;
        int[] intArray = Arrays.stream(indexAskedByAgent)
                .mapToInt(obj -> (int) obj)
                .toArray();
        System.out.println("the arguments are" + Arrays.toString(intArray));
        try{
            if (distanceGrid == null) throw new RuntimeException("Eror please call agent call before");
            double value = new Random().nextDouble();
            System.out.println("from the place " + value + " " + getIndex()[0] + " " + getSize()[0]);
            return distanceGrid[intArray[0]][intArray[1]];
        }catch(RuntimeException exception){
            exception.printStackTrace();
            return Double.MAX_VALUE;
        }
    }


    @Override
    public Object callMethod(int functionId, Object argument) {
        switch (functionId){
            case 0: { init(argument); break; }
            case 1: { computeBestAgentDistance(); break; }
            case 2: { computeProximityPolygons(argument); break; }
            case 3: { return test(argument); }
            default : { break; }
        }
        return  null;
    }

}
