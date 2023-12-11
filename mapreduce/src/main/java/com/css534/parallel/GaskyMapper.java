package com.css534.parallel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *  First Mapper to Read the input file covering binary distances / for all the facilities
 */
public class GaskyMapper extends MapReduceBase implements Mapper<LongWritable, Text, MapKeys, MapValue> {

    private MapKeys keys;

    // compute the best min distance from left to right for a specific row
    private double[] getLeftDistance(double[] leftDistance, List<Double> gridRows){
        boolean isFavlFound = false;
        for (int i=0; i < gridRows.size(); i++){
            if (gridRows.get(i) == 1){
                // a favourable one
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

    // compute the best min distance from right to left for a specific row
    private double[] getRightDistance(double[] rightDistance, List<Double> gridRows){
        boolean isFavrFound = false;
        for (int i=gridRows.size()-1; i >=0; --i){
            if (gridRows.get(i) == 1){
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

    /*
        No implicit representation for the un fav types since they are implicitly present in map array
     */
    @Override
    public void map(LongWritable longWritable, Text binaryImageRowInput, OutputCollector<MapKeys, MapValue> outputCollector, Reporter reporter) throws IOException {
        String inputFeatureMatrix = binaryImageRowInput.toString();
        String[] distFavArray = inputFeatureMatrix.split("\\s+");
        if (distFavArray.length == 0) {
            System.out.println("Error in reading the feature values please erify");
            return;
        }

        String facilityName = distFavArray[0];
        boolean isUnfavorableFacility = facilityName.trim().indexOf("-") != -1 ? true : false;

        Integer matrixRowNumber = Integer.valueOf(distFavArray[1]);
        System.out.println("Processing the feature value as" + facilityName + " with the row number " + matrixRowNumber);

//        String[] binMatrixValues = Arrays.copyOfRange(distFavArray, 2, distFavArray.length);
        String binMatrixValues = distFavArray[distFavArray.length - 1];
        System.out.println("The Read binary Distance value is " + binMatrixValues);

        List<Double> gridRows = new ArrayList<>();
        for (int i=0 ; i < binMatrixValues.length(); i++)
            gridRows.add(
                    Double.valueOf(
                            Character.getNumericValue(binMatrixValues.charAt(i))
                    )
            );

        System.out.println("The facility doubl ematrix is " + gridRows);

        /*
            If the binary map array for a row has 0 values it means it has unfavourable facilites
            Else they are fav one's;
         */
        double[] leftDistance = new double[binMatrixValues.length()];
        double[] rightDistance = new double[binMatrixValues.length()];

        Arrays.fill(leftDistance, Double.MAX_VALUE);
        Arrays.fill(rightDistance, Double.MAX_VALUE);

        leftDistance = getLeftDistance(leftDistance, gridRows);
        rightDistance = getRightDistance(rightDistance, gridRows);

        // can also be implemented using a stack
        for (int i=0; i < binMatrixValues.length(); i++){
            leftDistance[i] = Double.min(leftDistance[i], rightDistance[i]);

        }

        // emit the k/v pairs for with the key havinf ((facility type, col), distance)) for shuffle sort phase.
        for (int values = 0; values < leftDistance.length; values++){
            // keep the grid as 0 index based for each 1.... n
            outputCollector.collect(new MapKeys(facilityName ,values + 1), new MapValue(leftDistance[values], matrixRowNumber));
        }
    }

}
