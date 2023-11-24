import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GaskySparkJob {
    public static void main(String[] args) {
        // Create a Spark context
        SparkConf conf = new SparkConf().setAppName("GaskySparkJob");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input data
        JavaRDD<String> inputData = sparkContext.textFile(args[0]);

        // Debug: Print the input data
        System.out.println("Debug: Input Data:");
        inputData.foreach(line -> System.out.println(line));

        // Process the input data and create key-value pairs
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Integer, Double>> result = inputData
                .flatMapToPair(line -> parseInputData(line));   

        // Debug: Print the transformed data
        System.out.println("Debug: Transformed Data:");
        result.foreach(tuple -> System.out.println(tuple));

        // Collect the result and print or save it as needed
        List<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>>> resultList = result.collect();
        for (Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>> tuple : resultList) {
            System.out.println(tuple);
        }

        // Stop the Spark context
        sparkContext.stop();
    }

    // FlatMap function to process input data and generate key-value pairs
    private static Iterator<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>>> parseInputData(String line) {
        System.out.println("Debug: Processing Line: " + line);

        String[] distFavArray = line.split("\\s+");
        List<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>>> result = new ArrayList<>();

        if (distFavArray.length > 0) {
            String facilityName = distFavArray[0];
            int matrixRowNumber = Integer.parseInt(distFavArray[1]);

            // Debug: Print facilityName and matrixRowNumber
            System.out.println("Debug: FacilityName: " + facilityName + ", MatrixRowNumber: " + matrixRowNumber);

            // Convert the strings to a list of strings
            List<String> binMatrixValues = Arrays.asList(Arrays.copyOfRange(distFavArray, 2, distFavArray.length));

            // Debug: Print binMatrixValues
            System.out.println("Debug: binMatrixValues: " + binMatrixValues);

            double[] leftDistance = new double[binMatrixValues.get(0).length()];
            double[] rightDistance = new double[binMatrixValues.get(0).length()];

            Arrays.fill(leftDistance, Double.MAX_VALUE);
            Arrays.fill(rightDistance, Double.MAX_VALUE);

            leftDistance = getLeftDistance(leftDistance, binMatrixValues);
            rightDistance = getRightDistance(rightDistance, binMatrixValues);

            // Debug: Print leftDistance and rightDistance
            System.out.println("Debug: leftDistance: " + Arrays.toString(leftDistance));
            System.out.println("Debug: rightDistance: " + Arrays.toString(rightDistance));

            for (int i = 0; i < binMatrixValues.get(0).length(); i++) {
                result.add(new Tuple2<>(new Tuple2<>(facilityName, i + 1),
                        new Tuple2<>(matrixRowNumber, Double.min(leftDistance[i],rightDistance[i]))));
            }
        }

        // Debug: Print the generated key-value pairs
        System.out.println("Debug: Generated Key-Value Pairs:");
        result.forEach(tuple -> System.out.println(tuple));

        return result.iterator();
    }

    private static double[] getLeftDistance(double[] leftDistance, List<String> gridRows) {
        boolean isFavlFound = false;
        for (int i = 0; i < gridRows.get(0).length(); i++) {
            if (gridRows.get(0).charAt(i) == '1') {
                leftDistance[i] = 0;
                isFavlFound = true;
            } else if (isFavlFound) {
                leftDistance[i] = leftDistance[i - 1] + 1;
            }
        }
        // // Debug: Print leftDistance in the for loop
        // System.out.println("Debug: LeftDistance in getLeftDistance:");
        // System.out.println(Arrays.toString(leftDistance));
        return leftDistance;
    }

    private static double[] getRightDistance(double[] rightDistance, List<String> gridRows) {
        boolean isFavrFound = false;
        for (int i = gridRows.get(0).length() - 1; i >= 0; --i) {
            if (gridRows.get(0).charAt(i) == '1') {
                rightDistance[i] = 0;
                isFavrFound = true;
            } else if (isFavrFound) {
                rightDistance[i] = rightDistance[i + 1] + 1;
            }
        }
        //    // Debug: Print rightDistance in the for loop
        // System.out.println("Debug: RightDistance in getRightDistance:");
        // System.out.println(Arrays.toString(rightDistance));
        return rightDistance;
    }
}
