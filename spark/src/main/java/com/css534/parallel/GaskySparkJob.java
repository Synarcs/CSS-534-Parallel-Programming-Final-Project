import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import java.util.Collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import com.google.common.collect.Lists;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class GaskySparkJob {
    private static final int GRID_SIZE = 8; 

    public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("GaskySparkJob");

    try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {

        JavaRDD<String> inputData = sparkContext.textFile(args[0]);

        // Start a timer
        long startTime = System.currentTimeMillis();

        JavaPairRDD<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Double>>> result = inputData
                .flatMapToPair(line -> parseInputData(line))
                .groupByKey();

        List<Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Double>>>> resultList = result.collect();

        // Create a new list with the sorted elements
        List<Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Double>>>> sortedResultList = new ArrayList<>(resultList);
        sortedResultList.sort(Comparator.comparing(
                (Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Double>>> tuple) -> tuple._1(),
                Tuple2Comparator.INSTANCE
        ));

        List<Tuple2<String, Integer>> keysList = new ArrayList<>();
        List<Iterable<Tuple2<Integer, Double>>> valuesList = new ArrayList<>();

        for (Tuple2<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Double>>> tuple : sortedResultList) {
            keysList.add(tuple._1());
            valuesList.add(tuple._2());
        }
        
        List<Tuple2<Tuple2<String, Integer>, Tuple2<List<Double>, List<Tuple2<Double, Double>>>>> skylineObjectsList =
                applyGaskyAlgorithm(sparkContext, keysList, valuesList);

        long endTime = System.currentTimeMillis();
        System.out.println("Execution time = " + (endTime - startTime));

    } catch (Exception e) {
        e.printStackTrace();
    }
}

    private static List<Tuple2<Tuple2<String, Integer>, Tuple2<List<Double>, List<Tuple2<Double, Double>>>>> applyGaskyAlgorithm(JavaSparkContext sparkContext, List<Tuple2<String, Integer>> keysList, List<Iterable<Tuple2<Integer, Double>>> valuesList) {
    // Create JavaRDDs for keys and values
    JavaRDD<Tuple2<String, Integer>> keysRDD = sparkContext.parallelize(keysList);
    JavaRDD<Iterable<Tuple2<Integer, Double>>> valuesRDD = sparkContext.parallelize(valuesList);

    // Zip the two RDDs to create a PairRDD
    JavaPairRDD<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Double>>> zippedRDD = keysRDD.zip(valuesRDD);

    // Filter out points with Double.MAX_VALUE distance
    JavaPairRDD<Tuple2<String, Integer>, Iterable<Tuple2<Integer, Double>>> filteredRDD = zippedRDD.mapValues(values ->
            filterUnfavorablePoints(values)
    );

    // Apply gaskyAlgorithm to the remaining points
    return filteredRDD.map(tuple -> {
        Tuple2<String, Integer> keysTuple = tuple._1();
        int colNumber = keysTuple._2();
        Iterable<Tuple2<Integer, Double>> remainingPoints = tuple._2();

        //"Applying gaskyAlgorithm for Column number
        // Assuming gaskyAlgorithm takes an Iterable<Tuple2<Integer, Double>> and a Tuple2<String, Integer>
        Tuple2<List<Double>, List<Tuple2<Double, Double>>> skylineObjects = gaskyAlgorithm(remainingPoints, colNumber);
        return new Tuple2<>(keysTuple, skylineObjects);
    }).collect();

}

    private static Iterable<Tuple2<Integer, Double>> filterUnfavorablePoints(Iterable<Tuple2<Integer, Double>> values) {
        List<Tuple2<Integer, Double>> filteredValues = new ArrayList<>();
        for (Tuple2<Integer, Double> value : values) {
            if (!value._2().equals(Double.MAX_VALUE)) {
                filteredValues.add(value);
            }
        }
        return filteredValues;
    }



    private static Tuple2<List<Double>,List<Tuple2<Double, Double>>> gaskyAlgorithm(Iterable<Tuple2<Integer, Double>> remainingPoints, int colNumber) {
    int totalRemainingPoints = Iterables.size(remainingPoints);
    List<Double> distances = new ArrayList<>(Collections.nCopies(GRID_SIZE, Double.MAX_VALUE));
    if (totalRemainingPoints > 2) {
    List<Tuple2<Double, Double>> points = new ArrayList<>();

    // Convert remainingPoints to Tuple2<Double, Double> objects
    for (Tuple2<Integer, Double> point : remainingPoints) {
        points.add(new Tuple2<>(point._1().doubleValue(), point._2()));
    }

    // Filtering based on dominance
    int currentWindowStart = 1;
    while (points.size() >= 3 && currentWindowStart <= points.size() - 2) {  
        Tuple2<Double, Double> ii = points.get(currentWindowStart - 1);
        Tuple2<Double, Double> jj = points.get(currentWindowStart);
        Tuple2<Double, Double> kk = points.get(currentWindowStart + 1);

        if (ii != null && jj != null && kk != null) {
            double xij = calcBisectorProjections(ii._1(), ii._2(), jj._1(), jj._2())._1();
            double xjk = calcBisectorProjections(jj._1(), jj._2(), kk._1(), kk._2())._1();

            if (xij > xjk) {
               
                // Remove the middle point
                points.remove(currentWindowStart);
            } else {
                // Move to the next window
                currentWindowStart++;
            }
        }
    }
    // Call the method to find proximal points
    List<double[]> proximityProjectionsPoints = findProximityPoints(points, totalRemainingPoints, GRID_SIZE);

    int unDominatedPointsSize = points.size();
    int proximityIntervals = proximityProjectionsPoints.size() - 1;
    int dominatedCoordinatesDistances = 0;


    //update distance implementation
    for (int interval = 0; interval < proximityProjectionsPoints.size(); interval++) {
            double[] currentInterval = proximityProjectionsPoints.get(interval);
            Tuple2<Double,Double> dominantPoint = points.get(dominatedCoordinatesDistances);
            int start = (int) currentInterval[0];
            int end = (int) currentInterval[1];
            for (int xCord = start; xCord <= end; xCord++) {
                    if (distances.get(xCord - 1) != Double.MAX_VALUE){
                        distances.set(
                                xCord - 1,
                                Double.min(
                                        findEuclideanDistance(xCord, 0, dominantPoint._1(), dominantPoint._2()),
                                        distances.get(xCord - 1)
                                )
                        );
                    }else
                        distances.set(
                                xCord - 1,
                                findEuclideanDistance(xCord, 0, dominantPoint._1(), dominantPoint._2())
                        );
                }
                dominatedCoordinatesDistances++;
        }

        for (Tuple2<Double, Double> point : points) {
            double x = point._1();
            Tuple2<Double, Double> updatedPoint = new Tuple2<>(x, (double) colNumber);
            // Replace the existing point with the updated one
            points.set(points.indexOf(point), updatedPoint);
            }
    // check for the points based on the dominance
            return new Tuple2<List<Double>, List<Tuple2<Double, Double>>>(distances, points);


        }
        else{
            return new Tuple2<>(
            Collections.emptyList(),  // Empty list for distances
            Collections.emptyList()   // Empty list for points
    );
        }
}

    private static List<double[]> findProximityPoints(List<Tuple2<Double, Double>> unDominatedPoints, final int totalPoints, final int gridSize) {
    List<Tuple2<Double, Double>> intervals = new ArrayList<>();

    // Calculate intervals based on the unDominatedPoints
    for (int i = 1; i < unDominatedPoints.size(); i++) {
        Tuple2<Double, Double> point1 = unDominatedPoints.get(i - 1);
        Tuple2<Double, Double> point2 = unDominatedPoints.get(i);
        intervals.add(new Tuple2<>((point1._1() + point2._1()) / 2, 0.0));
    }

    // Combine intervals using a frame
    List<double[]> mergedInterval = new ArrayList<>(intervals.size());
    mergedInterval.add(new double[]{1, intervals.get(0)._1()});

    for (int i = 1; i < intervals.size(); i++) {
        mergedInterval.add(new double[]{intervals.get(i - 1)._1(), intervals.get(i)._1()});
    }

    mergedInterval.add(new double[]{intervals.get(intervals.size() - 1)._1(), GRID_SIZE});

    return mergedInterval;
}

    private static double findEuclideanDistance(int x, int y, int x1, int y1) {
        return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));
    }

    private static double findEuclideanDistance(double x, double y, double x1, double y1){
        return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));
    }

    // FlatMap function to process input data and generate key-value pairs
    private static Iterator<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>>> parseInputData(String line) {

        String[] distFavArray = line.split("\\s+");
        List<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>>> result = new ArrayList<>();

        if (distFavArray.length > 0) {
            String facilityName = distFavArray[0];
            int matrixRowNumber = Integer.parseInt(distFavArray[1]);

            // Convert the strings to a list of strings
            List<String> binMatrixValues = Arrays.asList(Arrays.copyOfRange(distFavArray, 2, distFavArray.length));

            double[] leftDistance = new double[binMatrixValues.get(0).length()];
            double[] rightDistance = new double[binMatrixValues.get(0).length()];

            Arrays.fill(leftDistance, Double.MAX_VALUE);
            Arrays.fill(rightDistance, Double.MAX_VALUE);

            leftDistance = getLeftDistance(leftDistance, binMatrixValues);
            rightDistance = getRightDistance(rightDistance, binMatrixValues);

            for (int i = 0; i < binMatrixValues.get(0).length(); i++) {
                result.add(new Tuple2<>(new Tuple2<>(facilityName, i + 1),
                        new Tuple2<>(matrixRowNumber, Double.min(leftDistance[i], rightDistance[i]))));
            }
        }


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
        return rightDistance;
    }

    // Comparator for Tuple2<String, Integer>
static class Tuple2Comparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    static final Tuple2Comparator INSTANCE = new Tuple2Comparator();

    @Override
    public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
        int compareResult = tuple1._1().compareTo(tuple2._1());
        if (compareResult == 0) {
            // If the first elements are equal, compare the second elements
            return Integer.compare(tuple1._2(), tuple2._2());
        }
        return compareResult;
    }
}
    
    private static Tuple2<Double, Double> calcBisectorProjections(double x, double y, double x1, double y1) {
        double xx = ((y1 * y1) - (y * y) + (x1 * x1) - (x * x)) / (2 * (x1 - x));
        double yy = 0;
        return new Tuple2<>(xx, yy);
    }

// Add this method to your class
private static <T> Iterable<T> iterableToJava(scala.collection.Iterable<T> scalaIterable) {
    List<T> javaList = new ArrayList<>();
    scala.collection.Iterator<T> scalaIterator = scalaIterable.iterator();
    while (scalaIterator.hasNext()) {
        javaList.add(scalaIterator.next());
    }
    return javaList;
}

}
