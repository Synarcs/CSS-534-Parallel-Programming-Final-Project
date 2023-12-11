package com.css534.parallel;//package com.css534.parallel;


import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

class GaskySequentialVr implements Serializable{

    private static boolean debug = false;
    public static class Minmax {
        double min;
        double max;
        int i;
        int j;
    }

    public static class Vector2f {
        private double xx;
        private double yy;

        public Vector2f(double xx, double yy) {
            this.xx = xx;
            this.yy = yy;
        }

        public double getXx() {
            return xx;
        }

        public double getYy() {
            return yy;
        }
    }

    public static class Vector2FProjections {
        private double xx;
        private double yy;

        public Vector2FProjections(double xx, double yy) {
            this.xx = xx;
            this.yy = yy;
        }

        public double getXx() {
            return xx;
        }

        public double getYy() {
            return yy;
        }

        public void setYy(double yy) {
            this.yy = yy;
        }

        public void setXx(double xx) {
            this.xx = xx;
        }
    }

    public static class SkylineObjects {
        private List<Double> distances;
        private List<Vector2f> skylineObjects;

        SkylineObjects() {
            this(new ArrayList<>(), new ArrayList<>());
        }

        SkylineObjects(List<Double> distances, List<Vector2f> skylineObjects) {
            this.distances = distances;
            this.skylineObjects = skylineObjects;
        }

        public List<Double> getDistances() {
            return distances;
        }

        public List<Vector2f> getSkylineObjects() {
            return skylineObjects;
        }
    }

    private static Vector2FProjections calcBisectorProjections(double x, double y, double x1, double y1) {

        double xx = ((y1 * y1) - (y * y) + (x1 * x1) - (x * x)) / (2 * (x1 - x));
        double yy = 0;

        Vector2FProjections vector2F = new Vector2FProjections(xx, yy);

        return vector2F;
    }

    private static double findEuclideanDistance(double x, double y, double x1, double y1) {
        return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));
    }

    private static Deque<Vector2f> findProximityPointsSingle(List<Vector2f> unDominatedPoints) {
        Deque<Vector2f> intervals = new LinkedList<>();
        for (int i = 1; i < unDominatedPoints.size(); i++) {
            Vector2f point1 = unDominatedPoints.get(i - 1);
            Vector2f point2 = unDominatedPoints.get(i);
            intervals.add(
                    new Vector2f(
                            (point1.getXx() + point2.getXx()) / 2,
                            0 // point lying on with intersection on X axis
                    ));
        }
        return intervals;
    }

    private static List<double[]> findProximityPoints(List<Vector2f> unDominatedPoints, int gridSize) {
        List<Vector2f> intervals = new ArrayList<>();
        for (int i = 1; i < unDominatedPoints.size(); i++) {
            Vector2f point1 = unDominatedPoints.get(i - 1);
            Vector2f point2 = unDominatedPoints.get(i);
            intervals.add(
                    new Vector2f(
                            (point1.getXx() + point2.getXx()) / 2,
                            0 // point lying on with intersection on X axis
                    ));
        }

        // implementation of combine intervals using the interval frame
        List<double[]> mergedInterval = new ArrayList<>(intervals.size());
        mergedInterval.add(new double[] { 1, intervals.get(0).getXx() });

        for (int i = 1; i < intervals.size(); i++) {
            mergedInterval.add(new double[] { intervals.get(i - 1).getXx(), intervals.get(i).getXx() });
        }

        mergedInterval.add(new double[] { intervals.get(intervals.size() - 1).getXx(), gridSize });
        return mergedInterval;
    }

    private static SkylineObjects mrGaskyAlgorithm(List<Vector2f> cartesianProjectPoints, int gridSize)
            throws RuntimeException, NoSuchElementException {
        int totalPoints = cartesianProjectPoints.size();
        List<Double> distances = new ArrayList<>(Collections.nCopies(gridSize, Double.MAX_VALUE));

        if (totalPoints > 2) {

            List<Vector2f> points = new LinkedList<>();

            for (int i = 0; i < cartesianProjectPoints.size(); i++)
                points.add(cartesianProjectPoints.get(i));

            // Filtering the points based on dominance to further calculate proximity
            // distance.
            int currentWindowStart = 1;
            while (points.size() >= 3 && currentWindowStart <= points.size() - 2) {
                Vector2f ii = points.get(currentWindowStart - 1);
                Vector2f jj = points.get(currentWindowStart);
                Vector2f kk = points.get(currentWindowStart + 1);

                if (ii != null && jj != null && kk != null) {
                    double xij = calcBisectorProjections(ii.getXx(), ii.getYy(), jj.getXx(), jj.getYy())
                            .getXx();
                    double xjk = calcBisectorProjections(jj.getXx(), jj.getYy(), kk.getXx(), kk.getYy())
                            .getXx();

                    if (xij > xjk) {
                        points.remove(currentWindowStart);
                        currentWindowStart++;
                    } else {
                        currentWindowStart++;
                    }
                }
            }

            List<double[]> proximityProjectionsPoints = findProximityPoints(points, gridSize);

            List<Double> testData = new ArrayList<>();
            for (double[] interval : proximityProjectionsPoints) {
                testData.add(interval[0]);
            }

            int unDominatedPointsSize = points.size();
            int proximityIntervals = proximityProjectionsPoints.size() - 1;

            // This will always be greater than 2 (since for this case we always have more
            // than 2 cartesian points in the grid).
            assert unDominatedPointsSize == proximityIntervals;
            int dominatedCoordinatesDistances = 0;

            for (int interval = 0; interval < proximityProjectionsPoints.size(); interval++) {
                double[] currentInterval = proximityProjectionsPoints.get(interval);
                int start = (int) currentInterval[0];
                int end = (int) currentInterval[1];
                Vector2f dominantPoint = points.get(dominatedCoordinatesDistances);

                // We only consider the int projection over x-axis for grid skyline
                for (int xCord = start; xCord <= end; xCord++) {
                    if (distances.get(xCord - 1) != Double.MAX_VALUE) {
                        distances.set(
                                xCord - 1,
                                Double.min(
                                        findEuclideanDistance(xCord, 0, dominantPoint.getXx(), dominantPoint.getYy()),
                                        distances.get(xCord - 1)));
                    } else
                        distances.set(
                                xCord - 1,
                                findEuclideanDistance(xCord, 0, dominantPoint.getXx(), dominantPoint.getYy()));
                }

                dominatedCoordinatesDistances++;
            }

            // Check for the points based on dominance
            return new SkylineObjects(
                    distances,
                    points);

        }

        Deque<Vector2f> proximityProjectionsPoints = findProximityPointsSingle(cartesianProjectPoints);
        if (proximityProjectionsPoints.size() == 0 && cartesianProjectPoints.size() == 1) {
            // Only one dominant point exists, hence has no partitions present
            for (int i = 1; i <= gridSize; i++) {
                distances.set(i - 1,
                        findEuclideanDistance(i, 0, cartesianProjectPoints.get(0).getXx(),
                                cartesianProjectPoints.get(0).getYy()));
            }
            return new SkylineObjects(
                    distances,
                    cartesianProjectPoints);
        } else if (proximityProjectionsPoints.size() == 0 &&
                cartesianProjectPoints.size() == 0) {
            // Nothing is present, all are dominated by each other
            double[] maxDistance = new double[gridSize];
            Arrays.fill(maxDistance, Double.MAX_VALUE);

            List<Double> maxDistanceList = new ArrayList<>();
            for (double distance : maxDistance) {
                maxDistanceList.add(distance);
            }

            return new SkylineObjects(maxDistanceList, new ArrayList<>());
        }

        /*
         * It has 2 dominated points in the grid and a single interval that is the
         * bisector of dominated coordinates
         * It is not possible to overlap over each other since the projection of these
         * points is over the cartesian x-axis grid
         */
        Vector2f intervalProjection = proximityProjectionsPoints.removeFirst();
        for (int i = 1; i <= gridSize; i++) {
            if (i == intervalProjection.getXx()) {
                distances.set(
                        i - 1,
                        Double.min(
                                findEuclideanDistance(i, 0, cartesianProjectPoints.get(0).getXx(),
                                        cartesianProjectPoints.get(0).getYy()),
                                findEuclideanDistance(cartesianProjectPoints.get(1).getXx(),
                                        cartesianProjectPoints.get(1).getYy(), i, 0)));
            } else if (i < intervalProjection.getXx()) {
                distances.set(i - 1,
                        findEuclideanDistance(i, 0, cartesianProjectPoints.get(0).getXx(),
                                cartesianProjectPoints.get(0).getYy()));
            } else if (i > intervalProjection.getXx()) {
                distances.set(i - 1,
                        findEuclideanDistance(cartesianProjectPoints.get(1).getXx(),
                                cartesianProjectPoints.get(1).getYy(), i, 0));
            }
        }
        return new SkylineObjects(
                distances, cartesianProjectPoints);
    }

    private static List<Map.Entry<Integer, Double>> getOrderedRowValues(int columnIndex, int m, int n,
                                                                        double[][] FacilityGrid) {
        List<Map.Entry<Integer, Double>> orderedMap = new ArrayList<>();

        // Iterate over the specified portion of FacilityGrid
        for (int row = 0; row < m; row++) {
            double distance = FacilityGrid[row][columnIndex];
            orderedMap.add(new AbstractMap.SimpleEntry<>(row + 1, distance));
        }

        return orderedMap;
    }

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        if (args.length != 5) {
            System.err.println("Usage: java MRGASKY <numberOfFacilities> size size favourablecount unfavourablecount");
            System.exit(1);
        }

        int numberOfFacilities = Integer.parseInt(args[0]);
        int m = Integer.parseInt(args[1]);
        int n = Integer.parseInt(args[2]);
        int fav = Integer.parseInt(args[3]);
        int unfav = Integer.parseInt(args[4]);

        double[][][] FacilityGrid = new double[fav + unfav][m][n];
        double[][][] FavourableFacilityGrid = new double[fav][m][n];
        double[][][] UnFavourableFacilityGrid = new double[unfav][m][n];

        try {
            String filePath = "input.txt";

            // Initialize the FacilityGrid array
            FacilityGrid = initializeArray(numberOfFacilities, m, n, filePath);

            // MRGASKY algorithm
            for (int facility = 0; facility < numberOfFacilities; facility++) {
                for (int row = 0; row < m; row++) {
                    // Step-1 Algorithm
                    // Calculate Euclidean distance from left to right
                    double[] dist_left_right = calculateDistanceLeftRight(FacilityGrid[facility][row]);

                    // Calculate Euclidean distance from right to left
                    double[] dist_right_left = calculateDistanceRightLeft(FacilityGrid[facility][row]);

                    // Update the grid based on the minimum distance
                    for (int col = 0; col < n; col++) {
                        FacilityGrid[facility][row][col] = Math.min(dist_left_right[col],
                                dist_right_left[col]);
                    }
                }
            }

            // Step-2 Algorithm
            // optimized algorithm implemented only for parallel purpose
            for (int facility = 0; facility < numberOfFacilities; facility++) {
                for (int column = 0; column < n; column++) {
                    // Get ordered row values for this rank
                    List<Map.Entry<Integer, Double>> orderedMap = getOrderedRowValues(column, m, n,
                            FacilityGrid[facility]);

                    int gridSize = orderedMap.size();

                    // Create a list to store Vector2f objects
                    List<Vector2f> cartesianProjections = new ArrayList<>();

                    // Iterate over orderedMap and create Vector2f objects
                    for (Map.Entry<Integer, Double> value : orderedMap) {
                        int key = value.getKey();
                        double val = value.getValue();

                        Vector2f vector = new Vector2f(key, val);
                        cartesianProjections.add(vector);
                    }

                    // Create a new list to store filtered Vector2f objects
                    List<Vector2f> filteredCartesianProjections = new ArrayList<>();

                    // Iterate over cartesianProjections and filter
                    for (Vector2f vector : cartesianProjections) {
                        if (vector.getYy() != Double.MAX_VALUE) {
                            filteredCartesianProjections.add(vector);
                        }
                    }

                    // Replace the original list with the filtered one
                    cartesianProjections = filteredCartesianProjections;

                    // Calculate skyline objects for this rank
                    SkylineObjects objects = mrGaskyAlgorithm(cartesianProjections, gridSize);

                    // Output results
                    StringBuilder totalDistances = new StringBuilder();

                    for (int i = 0; i < objects.getDistances().size(); i++) {
                        totalDistances.append(objects.getDistances().get(i));
                        totalDistances.append(" ");
                    }

                    // for (int i = 0; i < objects.getSkylineObjects().size(); i++) {
                    // totalDistances.append("(" + objects.getSkylineObjects().get(i).getXx() + ","
                    // + objects.getSkylineObjects().get(i).getYy() + ")");
                    // }

                    // Output results using rank as identifier
//                    System.out.println("Results from column " + column + ": " +
//                            totalDistances.toString());

                    // store the favourable and unfavourable facility grids
                    for (int row = 0; row < m; row++) {
                        if (facility < fav) {
                            FavourableFacilityGrid[facility][row][column] = objects.getDistances().get(row);
                        } else {
                            UnFavourableFacilityGrid[facility - fav][row][column] = objects.getDistances().get(row);
                        }
                    }
                }
            }

            if (debug){
                // print fav and unfav grids
                for (int facility = 0; facility < fav; facility++) {
                    System.out.println("Favourable Facility Grid " + facility + ":");
                    for (int row = 0; row < m; row++) {
                        for (int column = 0; column < n; column++) {
                            System.out.print(FavourableFacilityGrid[facility][row][column] + " ");
                        }
                        System.out.println();
                    }
                    System.out.println();
                }

                for (int facility = 0; facility < unfav; facility++) {
                    System.out.println("UnFavourable Facility Grid " + facility + ":");
                    for (int row = 0; row < m; row++) {
                        for (int column = 0; column < n; column++) {
                            System.out.print(UnFavourableFacilityGrid[facility][row][column] + " ");
                        }
                        System.out.println();
                    }
                    System.out.println();
                }
            }

            // Step-3 Algorithm - Global Skyline Objects
            List<Minmax> minmaxFavList = new ArrayList<>();

            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    Double local_min_val = Double.MAX_VALUE;
                    Double local_max_val = Double.MIN_VALUE;
                    Minmax minmax = new Minmax();
                    for (int k = 0; k < fav; k++) {
                        local_min_val = Double.min(local_min_val, FavourableFacilityGrid[k][i][j]);
                        local_max_val = Double.min(local_max_val, FavourableFacilityGrid[k][i][j]);
                    }
                    minmax.i = i;
                    minmax.j = j;
                    minmax.min = local_min_val;
                    minmax.max = local_max_val;
                    minmaxFavList.add(minmax);
                }
            }

            List<Minmax> minmaxUnFavList = new ArrayList<>();

            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    Double local_min_val = Double.MAX_VALUE;
                    Double local_max_val = Double.MIN_VALUE;
                    Minmax minmax = new Minmax();
                    for (int k = 0; k < unfav; k++) {
                        local_min_val = Double.min(local_min_val, UnFavourableFacilityGrid[k][i][j]);
                        local_max_val = Double.max(local_max_val, UnFavourableFacilityGrid[k][i][j]);
                    }
                    minmax.i = i;
                    minmax.j = j;
                    minmax.min = local_min_val;
                    minmax.max = local_max_val;
                    minmaxUnFavList.add(minmax);
                }
            }


            boolean isEqual = minmaxUnFavList.size() == minmaxFavList.size();
            if (isEqual) {
                boolean isGrid = minmaxUnFavList.size() == m * n;
                if (!isGrid) return;
            }else {
                System.out.println("error incorrect order of teh grid projected");
                return;
            }

            if (debug){
                System.out.println("min and max index 1  , 1 " + minmaxFavList.get(0).min + " " + minmaxFavList.get(0).max);
                System.out.println("min and max index 1  , 1 " + minmaxUnFavList.get(0).min + " " + minmaxUnFavList.get(0).max);
                System.out.println("the status for them is " + isEqual);
            }

            // the index at i, i holds the facility distance mini and max across all the facility
//            (1,1),,,,[(0,1.4142135623730951), (1,3.1622776601683795), (1,3.1622776601683795)] // same as key value reduced in saprk
//            the only difference is its combined

            // O (N)
            for (int i=0 ; i < m * n ; i++){
                // use the struct for faster memory processing as compared o memory jumps required for a 2d array case
                double globalMaxIndexFav = minmaxFavList.get(i).max;
                double globalMinimaIndexFav = minmaxFavList.get(i).min;
                int favi = minmaxFavList.get(i).i + 1; int favj = minmaxFavList.get(i).j + 1;

                double globalMinimaIndexUnFav = minmaxUnFavList.get(i).min;
                double globalMaxIndexUnFav = minmaxUnFavList.get(i).max;
//                int ui = minmaxUnFavList.get(i).i; int uf = minmaxUnFavList.get(i).j;
                // ui, uf will have same value as of favi

                if (globalMinimaIndexFav != Double.MAX_VALUE && globalMinimaIndexUnFav != Double.MAX_VALUE) {
                    if (globalMinimaIndexUnFav < globalMinimaIndexFav ||
                            globalMinimaIndexFav == globalMinimaIndexUnFav ||
                            globalMaxIndexFav > globalMinimaIndexUnFav ||
                            globalMinimaIndexFav > globalMaxIndexUnFav) {
                        continue;
                    } else {
                        System.out.println(favi + " " + favj);
                    }
                }
            }

            // Step-4 Algorithm - Global Skyline Objects
            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;
            System.out.println("Total Elapsed Time: " + elapsedTime + " milliseconds");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    private static double[][][] initializeArray(int numberOfFacilities, int m, int n, String filePath)
            throws IOException {
        double[][][] facilityGrid = new double[numberOfFacilities][m][n];
        int facilityIndex;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);

                // Read facility name, row, and grid values
                String facilityName = tokenizer.nextToken();
                int row = Integer.parseInt(tokenizer.nextToken()) - 1;

                // the grid values are in the format "00000000"
                String gridValues = tokenizer.nextToken();

                // Convert gridValues to a double array
                double[] rowValues = new double[gridValues.length()];
                for (int i = 0; i < gridValues.length(); i++) {
                    rowValues[i] = Character.getNumericValue(gridValues.charAt(i));
                }

                // Populate the facilityGrid array
                if (facilityName.contains("-")) {
                    facilityIndex = Integer.parseInt(facilityName.substring(1, 2)) - 1;
                } else {
                    facilityIndex = Integer.parseInt(facilityName.substring(1)) - 1;
                }

                for (int j = 0; j < n; j++) {
                    facilityGrid[facilityIndex][row][j] = rowValues[j];
                }
            }
        }

        return facilityGrid;
    }

    // Function to calculate Euclidean distance from left to right for a single row
    private static double[] calculateDistanceLeftRight(double[] row) {
        double[] dist_left_right = new double[row.length];
        double dist = Double.MAX_VALUE;
        for (int col = 0; col < row.length; col++) {
            if (row[col] == 1.0) {
                dist = 0.0;
            } else if (dist >= 0.0) {
                dist++;
            } else if (row[col] == 0.0) {
                dist = Double.MAX_VALUE;
            }

            dist_left_right[col] = dist;
        }

        return dist_left_right;
    }

    // Function to calculate Euclidean distance from right to left for a single row
    private static double[] calculateDistanceRightLeft(double[] row) {
        double[] dist_right_left = new double[row.length];
        double dist = Double.MAX_VALUE;
        for (int col = row.length - 1; col >= 0; col--) {
            if (row[col] == 1.0) {
                dist = 0.0;
            } else if (dist >= 0.0) {
                dist++;
            } else if (row[col] == 0.0) {
                dist = Double.MAX_VALUE;
            }
            dist_right_left[col] = dist;
        }
        return dist_right_left;
    }

}