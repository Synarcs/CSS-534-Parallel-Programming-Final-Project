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

        // create the final holder pplace to load all the arrays to the heap for the place thread
        facilityGridPlaces = new double[gridSizePerFacility[0]][gridSizePerFacility[1]];
        gridX = gridSizePerFacility[0];
        gridY = gridSizePerFacility[1];

        System.out.println(facilityGridPlaces.length + " " + facilityGridPlaces[0].length + " " + filename);

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

        if (!debug){
            for (int i = 0; i < gridX; i++){
                System.out.println(Arrays.toString(distanceGrid[i]));
            }
        }
    }

    private void computeProximityPolygons(Object argument){

    }

    public double loadDistance(Object argument){

        System.out.println(argument.getClass().getName() + argument.getClass().getCanonicalName());
        Object[] indexAskedByAgent = (Object[]) argument;
        int[] intArray = Arrays.stream(indexAskedByAgent)
                .mapToInt(obj -> (int) obj)
                .toArray();
        System.out.println("the arguments are" + Arrays.toString(intArray));
        try{
            if (distanceGrid == null) throw new RuntimeException("Eror please call agent call before");
            double value = new Random().nextDouble();
            System.out.println("from the place " + " " + getIndex()[0] + " " + getSize()[0]);
            return distanceGrid[intArray[0]][intArray[1]];
        }catch(RuntimeException exception){
            exception.printStackTrace();
            return Double.MAX_VALUE;
        }
    }

    public void loadBinaryMatrix(Object argument){

        String[] tokens = (String[]) argument;
        System.out.println("Binary tokens at this place" + Arrays.toString(tokens) + " " + getIndex()[0]);

        for (int i=0; i < tokens.length; i++){
            for (int j=0 ; j < tokens[0].length(); j++){
                facilityGridPlaces[i][j] = Double.valueOf(
                        Character.getNumericValue(tokens[i].charAt(j)));
            }
        }

        if (!debug){
            System.out.println("for place id " + getIndex()[0]);

            for (int i=0 ; i < facilityGridPlaces.length; i++){
                System.out.println(Arrays.toString(facilityGridPlaces[i]));
            }
        }


        return;
    }

    public void buffer(Object argument) {
        System.out.println("This place is at" + getIndex()[0] + "  " + getSize());
        System.out.println(argument.getClass().getCanonicalName());
        String[] info = (String[]) argument;
        for (String cc: info) System.out.println(cc);
    }

    @Override
    public Object callMethod(int functionId, Object argument) {
        switch (functionId){
            case 0: { init(argument); break; }
            case 11: { loadBinaryMatrix(argument); break; }
            case 1: { computeBestAgentDistance(); break; }
            case 2: { computeProximityPolygons(argument); break; }
            case 3: { return loadDistance(argument); }
            case 4: { buffer(argument); break;}
            default : { break; }
        }
        return  null;
    }

}
