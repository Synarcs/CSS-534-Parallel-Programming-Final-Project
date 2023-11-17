package com.css534.parallel;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GaskyMapper extends MapReduceBase implements Mapper<LongWritable, Text, MapKeys, DoubleWritable> {

    private MapKeys keys;

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

    // TODO: 16/11/23  need to add the feature implementation with feature type provided as input
    /*
        No implicit representation for the un fav types since they are implicitly present in map array
     */
    @Override
    public void map(LongWritable longWritable, Text binaryImageRowInput, OutputCollector<MapKeys, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        String inputFeatureMatrix = binaryImageRowInput.toString();
        String[] distFavArray = inputFeatureMatrix.split("\\s+");
        if (distFavArray.length == 0) return;

        String facilityName = distFavArray[0];
        Integer matrixRowNumber = Integer.valueOf(distFavArray[1]);
        System.out.println("Processing the feature value as" + facilityName + " with the row number " + matrixRowNumber);

        String[] binMatrixValues = Arrays.copyOfRange(distFavArray, 2, distFavArray.length);

        List<Double> gridRows = Arrays.stream(binMatrixValues)
                                      .map(Double::valueOf).toList();

        /*
            If the binary map array for a row has 0 values it means it has unfavourable facilites
            Else they are fav one's;
         */
        double[] leftDistance = new double[distFavArray.length];
        double[] rightDistance = new double[distFavArray.length];

        Arrays.fill(leftDistance, Double.MAX_VALUE);
        Arrays.fill(rightDistance, Double.MAX_VALUE);

        leftDistance = getLeftDistance(leftDistance, gridRows);
        rightDistance = getRightDistance(rightDistance, gridRows);

        // can also be implemented using a stack
        for (int i=0; i < gridRows.size(); i++){
            leftDistance[i] = Double.min(leftDistance[i], rightDistance[i]);
        }

        for (int values = 0; values < leftDistance.length; values++){
            // keep the grid as 0 index based for each
            outputCollector.collect(new MapKeys(facilityName,matrixRowNumber ,values + 1), new DoubleWritable(leftDistance[values]));
        }
    }

}
