package com.css534.parallel;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;


public class Main implements Serializable {
    private final static int clusterSize = 4;
    private final static boolean DEBUG = false;
    private static Logger log = LoggerFactory.getLogger(Main.class);

    public Main(){}


    private static Tuple2<Double, Double> calcBisectorProjections(double x, double y , double x1, double y1){
        double xx = ((y1 * y1 ) - (y * y) + (x1 * x1) - (x * x)) / 2 * (x1 - x);
        double yy = 0;
        return new Tuple2<>(
                xx, yy
        );
    }

    private static List<double[]> findProximityPoints(List<Tuple2<Double, Double>> unDominatedPoints, int gridSize) {
        List<Tuple2<Double, Double>> intervals = new ArrayList<>();
        for (int i=1; i < unDominatedPoints.size(); i++){
            Tuple2<Double, Double> point1 = unDominatedPoints.get(i-1);
            Tuple2<Double, Double> point2 = unDominatedPoints.get(i);
            intervals.add(
                    new Tuple2<Double, Double>(
                            (point1._1() + point2._1()) / 2,
                            0.0 // point lying on with intersection on X axis
                    )
            );
        }
        // i can do it in O(1) space, lazy to do it lol
        // implementation of combine intervals using a the interval frame
        List<double[] > mergedInterval = new ArrayList<>(intervals.size());
        mergedInterval.add(
                new double[]{1, intervals.get(0)._1()}
        );
        for (int i = 1; i < intervals.size(); i++){
            mergedInterval.add(
                    new double[]{intervals.get(i - 1)._1(), intervals.get(i)._1()}
            );
        }
        mergedInterval.add(
                new double[]{intervals.get(intervals.size() - 1)._1(), gridSize}
        );
        return mergedInterval;
    }

    private static Deque<Tuple2<Double, Double>> findProximityPointsSingle(List<Tuple2<Double, Double>> unDominatedPoints){
        Deque<Tuple2<Double, Double>> intervals = new LinkedList<>();
        for (int i=1; i < unDominatedPoints.size(); i++){
            Tuple2<Double, Double> point1 = unDominatedPoints.get(i-1);
            Tuple2<Double, Double> point2 = unDominatedPoints.get(i);
            intervals.add(
                    new Tuple2<Double, Double>(
                            (point1._1() + point2._1()) / 2,
                            0.0 // point lying on with intersection on X axis
                    )
            );
        }
        return intervals;
    }

    private static double findEuclideanDistance(int x, int y, int x1, int y1){return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));}

    private static double findEuclideanDistance(double x, double y, double x1, double y1){return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));}

    /*
        This will run for each column and determine the closest distance from skyline objects
        This will all return the final skyline objects if any
     */
    public static List<Double> mrGaskyAlgorithm(Iterable<Tuple2<Double, Double>> cartesianProjectPoints, int gridSize) throws RuntimeException, NoSuchElementException {
        int totalPoints = 0;
        for (Tuple2<Double, Double> points: cartesianProjectPoints) totalPoints++;
        List<Double> distances = new ArrayList<>(Collections.nCopies(gridSize, Double.MAX_VALUE));

        if (totalPoints > 2) {
            log.info("The current length of the un dominated grids is" + totalPoints);
            log.info("This is a non implemented method yet");

            List<Tuple2<Double, Double>> points = new LinkedList<>();

            for (Tuple2<Double, Double> point: cartesianProjectPoints)
                points.add(point);

            // filtering the points based on the dominance to further calculate proximity distance
            int currentWindowStart = 1;
            while (points.size() >= 3 && currentWindowStart <= points.size() - 2) {
                Tuple2<Double, Double> ii = points.get(currentWindowStart - 1);
                Tuple2<Double, Double> jj = points.get(currentWindowStart);
                Tuple2<Double, Double> kk = points.get(currentWindowStart + 1);

                if (ii != null && jj != null && kk != null) {
                    double xij = calcBisectorProjections(ii._1(), ii._2(), jj._1(), jj._2())._1();
                    double xjk = calcBisectorProjections(jj._1(), jj._2(), kk._1(), kk._2())._1();

                    if (xij > xjk) {
                        points.remove(currentWindowStart);
                        currentWindowStart++;
                    } else {
                        currentWindowStart++;
                    }
                }
            }

            log.info("The current remained dominated points are");
            List<double[]> proximityProjectionsPoints = findProximityPoints(points, gridSize);
            List<Double> testData = proximityProjectionsPoints.stream().map((i) -> i[0]).collect(Collectors.toList());

            int unDominatedPointsSize = points.size();
            int proximityIntervals = proximityProjectionsPoints.size() - 1;
            // // this will always be greater than 2 (since for this case we alwasy have more than 2 cartesian points in the grid).
            assert  unDominatedPointsSize == proximityIntervals;
            int dominatedCoordinatesDistances = 0;
//          [ [1, 3.0] [3, 5.0] [5, 8]] || (2.0,3.0)(4.0,1.0)(6.0,6.0)

            for (int interval=0; interval < proximityProjectionsPoints.size(); ++interval){
                double[] currentInterval = proximityProjectionsPoints.get(interval);
                int start = (int) currentInterval[0]; int end = (int) currentInterval[1];
                Tuple2<Double, Double> dominantPoint = points.get(dominatedCoordinatesDistances);
                // we only consider the int projection over x asix for grid skyline
                for (int xCord=start; xCord <= end; xCord++){
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


            // check for the points based on the dominance
            return distances;
        }

        // sice the points are less we wont take more memory in the heap and can be converted into simple list
        List<Tuple2<Double, Double>> cartesianProjectPointsProjected = new ArrayList<>();
        for (Tuple2<Double, Double> points: cartesianProjectPoints) cartesianProjectPointsProjected.add(points);

        Deque<Tuple2<Double, Double>> proximityProjectionsPoints = findProximityPointsSingle(cartesianProjectPointsProjected);
        if (proximityProjectionsPoints.size() == 0 && totalPoints == 1){
            // only one dominant point exist hence has no partitions present
            for (int i = 1; i <= gridSize; i++){
                distances.set(i-1,
                        findEuclideanDistance(i, 0, cartesianProjectPointsProjected.get(0)._1(), cartesianProjectPointsProjected.get(0)._2())
                );
            }
            return distances;
        }
        else if (proximityProjectionsPoints.size() == 0 && totalPoints == 0) {
            // nothing is present all are dominated by each other
            double[] maxDistance = new double[gridSize];
            Arrays.fill(maxDistance, Double.MAX_VALUE);
            return Arrays.stream(maxDistance).boxed().collect(Collectors.toList());
        }

        /*
            it has 2 dominated points in the grid and single interval that is bisector of dominated coordinates
            It is not possible to overlap over each other since the projection of these points is over cartesian x asis grid
        */
        Tuple2<Double, Double> intervalProjection = proximityProjectionsPoints.removeFirst();
        for (int i=1; i <= gridSize ; i++) {
            if (i ==  intervalProjection._1()){
                distances.set(
                        i - 1,
                        Double.min(
                                findEuclideanDistance(i, 0, cartesianProjectPointsProjected.get(0)._1(), cartesianProjectPointsProjected.get(0)._2()),
                                findEuclideanDistance(cartesianProjectPointsProjected.get(1)._1(), cartesianProjectPointsProjected.get(1)._2(), i, 0)
                        )
                );
            }else if (i < intervalProjection._1()){
                distances.set(i - 1,
                        findEuclideanDistance(i, 0, cartesianProjectPointsProjected.get(0)._1(), cartesianProjectPointsProjected.get(0)._2())
                );
            }else if (i > intervalProjection._1()){
                distances.set(i - 1,
                        findEuclideanDistance(cartesianProjectPointsProjected.get(1)._1(), cartesianProjectPointsProjected.get(1)._2(), i, 0)
                );
            }
        }
        return distances;
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

    private static Iterator<Tuple2<Tuple2<String, Integer>, Tuple2<Double, Double>>> parseInputData(String line) {

        String[] distFavArray = line.split("\\s+");
        List<Tuple2<Tuple2<String, Integer>, Tuple2<Double, Double>>> result = new ArrayList<>();

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

            for (int i = 0; i < binMatrixValues.get(0).length(); ++i) {
                result.add(new Tuple2<>(new Tuple2<>(facilityName, i + 1),
                        new Tuple2<>(Double.valueOf(matrixRowNumber), Double.min(leftDistance[i], rightDistance[i]))));
            }
        }

        return result.iterator();
    }



    private static void debug(JavaPairRDD<Tuple2<String, Integer>, Iterable<Tuple2<Double, Double>>> rdd){
        rdd.collect().forEach(tuple -> {
            Tuple2<String, Integer> key = tuple._1();
            Iterable<Tuple2<Double, Double>> values = tuple._2();

            System.out.println("Key: " + key.toString());

            for (Tuple2<Double, Double> value : values) {
                System.out.println("  Value: " + value.toString());
            }
        });
    }

    private static int getGridSize(List<String> fileLineInput) throws RuntimeException {
        String[] input = fileLineInput.get(0).split("\\s+");
        if (input.length == 0 && input.length != 3)
            throw new RuntimeException();

        return input[input.length - 1].length();
    }
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("GaskySparkJob")
                .set("spark.executor.cores", "4")  // Set the number of cores per executor
                .set("spark.executor.instances", String.valueOf(clusterSize))  // Set the number of executor instances
                .set("spark.executor.memory", "4g");  // Set the executor memory

        System.out.println(Arrays.toString(args));
        String fileName = args[0];
        long startTime = System.currentTimeMillis();

        try {
            JavaSparkContext sc = new JavaSparkContext(conf);
            sc.setLogLevel("WARN");

            int numCores = Integer.parseInt(sc.getConf().get("spark.executor.cores")) *
                    Integer.parseInt(sc.getConf().get("spark.executor.cores"));
            long totalMemory = Long.parseLong(sc.getConf().get("spark.executor.memory").replace("g", "")) *
                    Integer.parseInt(sc.getConf().get("spark.executor.instances"));

            numCores = numCores * clusterSize;

            // read the file from hdfs later
            JavaRDD<String> inputData = sc.textFile(fileName).repartition(numCores);

            System.out.println("the grid size for the skyline objects is " + Integer.parseInt(args[1]));

            JavaPairRDD<Tuple2<String, Integer>, Iterable<Tuple2<Double, Double>>> data = inputData
                    .flatMapToPair(fileLine -> parseInputData(fileLine))
                    .groupByKey();

            System.out.println("value for the data is" + data.count());

            data = data.mapValues((Iterable<Tuple2<Double, Double>> iterable) -> {
                List<Tuple2<Double, Double>> list = new ArrayList<>();
                iterable.forEach(list::add);
                list.sort(Comparator.comparing(Tuple2::_1));
                return list;
            });

            data = data.mapValues((Iterable<Tuple2<Double, Double>> columnProjection) -> {
                List<Tuple2<Double, Double>> filteredValue = new ArrayList<>();
                for (Tuple2<Double, Double> value : columnProjection) {
                    if (value._2() != Double.MAX_VALUE) {
                        filteredValue.add(value);
                    }
                }
                return filteredValue;
            });

            if (DEBUG)
                debug(data);

            JavaPairRDD<Tuple2<String, Integer>, List<Double>> colProjections = data.mapValues((Iterable<Tuple2<Double, Double>> columnProjection) -> {
                return mrGaskyAlgorithm(columnProjection, Integer.parseInt(args[1])).iterator();
            }).mapValues((Iterator<Double> iter) -> {
                List<Double> list = new ArrayList<>();
                while (iter.hasNext()) {
                    list.add(iter.next());
                }
                return list;
            });

            if (DEBUG)
                colProjections.collect().forEach(v -> {
                    System.out.println(v._1._1 + " " + v._1._2 + " --> " + v._2);
                });


            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double>>> mergedGlobalReduce =
                    colProjections.flatMapToPair((Tuple2<Tuple2<String, Integer>, List<Double>> value) -> {
                        Tuple2<String, Integer> key = value._1();
                        List<Double> distances = value._2();

                        int colNumber = key._2();
                        String facilityName = key._1();
                        boolean isUnfavorableFacility = facilityName.trim().indexOf("-") != -1 ? true : false;

                        // col row ,                    fac type    distance
                        List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>> listFlatten = new ArrayList<>();

                        for (int row=0; row < distances.size(); row++){
                            if (isUnfavorableFacility){
                                listFlatten.add(
                                        new Tuple2<>(
                                                new Tuple2<>(colNumber, row + 1),
                                                new Tuple2<>(0, distances.get(row))
                                        )
                                );
                            }else {
                                listFlatten.add(
                                        new Tuple2<>(
                                                new Tuple2<>(colNumber, row + 1),
                                                new Tuple2<>(1, distances.get(row))
                                        )
                                );
                            }
                        }
                        return listFlatten.iterator();
                    }).groupByKey();
            // .sortByKey(Comparator.comparing(Tuple2::_2), true, numCores);

            if (DEBUG){
                mergedGlobalReduce.take(1).forEach((Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Double>>> value) -> {
                    System.out.println(value._1()._1() + " " + value._1()._2());
                    for (Tuple2<Integer, Double> pres: value._2()){
                        System.out.println(pres + "\t");
                    }
                });
            }

            System.out.println("Applying global Skyline Point Reduction on grid");

            if (DEBUG)
                mergedGlobalReduce.collect().forEach((reducedProjections) -> {
                    System.out.println(reducedProjections._1() + ",,,," + reducedProjections._2());
                });

            // did not use flatmapto pair for memory efficienty and less requirement for shuffle sort on the rdd
            JavaPairRDD<Tuple2<Integer, Integer>, Boolean> filterDominantGlobalPoints = mergedGlobalReduce.mapValues((Iterable<Tuple2<Integer, Double>> projectedPointPlane) -> {
                double globalMaxIndexFav = Double.MIN_VALUE;
                double globalMaxIndexUnFav = Double.MIN_VALUE;

                double globalMinimaIndexFav = Double.MAX_VALUE;
                double globalMinimaIndexUnFav = Double.MAX_VALUE;

                int FACILITY_COUNT = 2;
                List<Double> processGridData[] = new ArrayList[FACILITY_COUNT];
                for (int i=0; i < processGridData.length; i++) processGridData[i] = new ArrayList<>();

                final int FAVOURABLE_POSITION = 1;
                final int UNFAVOURABLE_POSITION = 0;
                // facility Type    min Distance suggested by projection
                for (Tuple2<Integer, Double> combinedCoordinateprojection: projectedPointPlane){
                    if (combinedCoordinateprojection._1() == FAVOURABLE_POSITION)
                        processGridData[0].add(
                                combinedCoordinateprojection._2()
                        );
                    else
                        processGridData[1].add(
                                combinedCoordinateprojection._2()
                        );
                }

                for (Double fd: processGridData[0]){
                    globalMinimaIndexFav = Double.min(globalMinimaIndexFav, fd);
                    globalMaxIndexFav = Double.min(globalMaxIndexFav, fd);
                }

                for (Double ud: processGridData[1]){
                    globalMinimaIndexUnFav = Double.min(globalMinimaIndexUnFav, ud);
                    globalMaxIndexUnFav = Double.max(globalMaxIndexUnFav, ud);
                }

                if (globalMinimaIndexFav != Double.MAX_VALUE && globalMinimaIndexUnFav != Double.MAX_VALUE){
                    if (globalMinimaIndexUnFav < globalMinimaIndexFav ||
                            globalMinimaIndexFav == globalMinimaIndexUnFav ||
                            globalMaxIndexFav > globalMinimaIndexUnFav ||
                            globalMinimaIndexFav > globalMaxIndexUnFav){
                        log.info("The minimum scale object with unFav facility more closer");
                        return false;
                    }else {
                        return true;
                    }
                }
                return false;
            });

            if (DEBUG)
                filterDominantGlobalPoints.collect().forEach((reducedProjections) -> {
                    System.out.println(reducedProjections._1() + "<--->" + reducedProjections._2());
                });


            filterDominantGlobalPoints = filterDominantGlobalPoints.filter((Tuple2<Tuple2<Integer, Integer>, Boolean> value) -> value._2());

            JavaPairRDD<Integer, Integer> transformedCoordinates = filterDominantGlobalPoints.mapToPair((Tuple2<Tuple2<Integer, Integer>, Boolean> value) -> {
                int colProjection = value._1()._1;
                int rowProjection = value._1()._2;

                return new Tuple2<Integer, Integer>(
                        rowProjection,
                        colProjection
                );
            });

            JavaPairRDD<Integer, Integer> orderedCoordinateProjection = transformedCoordinates.sortByKey()
                    .groupByKey()
                    .mapValues((orderedColumnProjection) -> {
                        List<Integer> sortedValues = new ArrayList<>();
                        orderedColumnProjection.forEach(sortedValues::add);
                        Collections.sort(sortedValues);
                        return sortedValues;
                    })
                    .flatMapToPair((Tuple2<Integer, List<Integer>> value) -> {

                        List<Tuple2<Integer, Integer>> resultOdering = new ArrayList<>();
                        int currentOrderedRowProjection = value._1();

                        for (Integer colProjection: value._2()){
                            resultOdering.add(
                                    new Tuple2<>(
                                            currentOrderedRowProjection,
                                            colProjection
                                    )
                            );
                        }
                        return resultOdering.iterator();
                    });


            orderedCoordinateProjection.collect().forEach((reducedProjections) -> {
                // projecting the final orderProjection of coordinates
                System.out.println(
                        reducedProjections._1() + "  " + reducedProjections._2()
                );
            });

            System.out.println("Elasped Time: " + (System.currentTimeMillis() - startTime));
        }catch (Exception exception){
            System.out.println("error in processing the spark context");
            exception.printStackTrace();
        }
    }
}
