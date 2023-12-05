package com.css534.parallel;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

import mpi.*;

class MRGASKYMPI {
    private static boolean debug = false;
    public static class Minmax implements Serializable {
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

    private List<Vector2f> getSkylinePoints(double[][] grid) {
        List<Vector2f> skylinePoints = new ArrayList<>();
        for (int col = 0; col < grid[0].length; col++) {
            for (int row = 0; row < grid.length; row++) {
                if (grid[row][col] == 1) {
                    skylinePoints.add(new Vector2f(col + 1, row + 1));
                    break; // Assuming there's at most one skyline point per column
                }
            }
        }
        return skylinePoints;
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
            // distance. Need to check while loop condition. we can use
            // cartesianProjectPoints.size in while.
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
                                                                        double[][] FacilityGrid, int rank) {
        List<Map.Entry<Integer, Double>> orderedMap = new ArrayList<>();

        // Iterate over the specified portion of FacilityGrid
        for (int row = 0; row < m; row++) {
            double distance = FacilityGrid[row][columnIndex];
            orderedMap.add(new AbstractMap.SimpleEntry<>(row + 1, distance));
        }

        return orderedMap;
    }

    public static void main(String[] args) throws MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        if (rank == 0 && args.length != 3) {
            System.err.println("Usage: java MRGASKYMPI <numberOfFacilities> size size favourable unfavourable");
            MPI.Finalize();
            System.exit(1);
        }

        int[] params = new int[5];
        if (rank == 0) {
            params[0] = Integer.parseInt(args[0]);
            params[1] = Integer.parseInt(args[1]);
            params[2] = Integer.parseInt(args[2]);
            params[3] = Integer.parseInt("2");
            params[4] = Integer.parseInt("1");
        }

        MPI.COMM_WORLD.Bcast(params, 0, 5, MPI.INT, 0);

        int numberOfFacilities = params[0];
        int m = params[1];
        int n = params[2];
        int fav = params[3];
        int unfav = params[4];

        System.out.println("rank:" + rank + " , numberOfFacilities:" +
                numberOfFacilities);
        System.out.println("rank:" + rank + " , m:" + m);
        System.out.println("rank:" + rank + " , n:" + n);
        System.out.println("rank:" + rank + " , size:" + size);
        System.out.println("rank:" + rank + " , favroiurable:" + fav);
        System.out.println("rank:" + rank + " , unfavroiurable:" + unfav);

        int facilitiesPerRank = numberOfFacilities / size;
        int startIndex = rank * facilitiesPerRank;
        int endindex = startIndex + facilitiesPerRank;
        int remainder = m % size;
        long startTime = System.currentTimeMillis();

        double[][][] FacilityGrid = new double[facilitiesPerRank][m][n];

        try {
            String filePath = "input.txt";
            FacilityGrid = initializeArray(facilitiesPerRank, m, n, rank, filePath);

            System.out.println("Running computation on Rank-" + rank);
            // MRGASKY algorithm
            for (int facility = 0; facility < facilitiesPerRank; facility++) {
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
            for (int facility = 0; facility < facilitiesPerRank; facility++) {
                for (int column = 0; column < n; column++) {
                    // Get ordered row values for this rank
                    List<Map.Entry<Integer, Double>> orderedMap = getOrderedRowValues(column, m, n,
                            FacilityGrid[facility],
                            rank);

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

                    for (int i = 0; i < objects.getDistances().size(); i++) {
                        FacilityGrid[facility][i][column] = objects.getDistances().get(i);
                    }
                }
            }

            // print fav and unfav grids
            for (int facility = 0; facility < facilitiesPerRank; facility++) {
                System.out.println("Facility Grid:" + facility + " at Rank:" + rank + ":");
                for (int row = 0; row < m; row++) {
                    for (int column = 0; column < n; column++) {
                        System.out.print(FacilityGrid[facility][row][column] + " ");
                    }
                    System.out.println();
                }
                System.out.println();
            }

            // Step-3 Algorithm - Global Skyline Objects
            Minmax[] minmaxArray = new Minmax[m * n];
            int index = 0;

            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    Double local_min_val = Double.MAX_VALUE;
                    Double local_max_val = Double.MIN_VALUE;
                    Minmax minmax = new Minmax();
                    for (int k = 0; k < facilitiesPerRank; k++) {
                        local_min_val = Double.min(local_min_val, FacilityGrid[k][i][j]);
                        local_max_val = Double.max(local_max_val, FacilityGrid[k][i][j]);
                    }
                    minmax.i = i;
                    minmax.j = j;
                    minmax.min = local_min_val;
                    minmax.max = local_max_val;
                    minmaxArray[index++] = minmax;
                }
            }

            // add barrier
            MPI.COMM_WORLD.Barrier();

            // print minmaxArray
            for (Minmax minmax : minmaxArray) {
                System.out.println("Rank:" + rank + " Min:" + minmax.min + " Max:" + minmax.max + " i:" + minmax.i
                        + " j:" + minmax.j);
            }

            if (rank == 0) {
                Minmax[][] minmaxFavArray = new Minmax[size-1][m * n];
                Minmax[][] minmaxUnFavArray = new Minmax[1][m * n];

                minmaxFavArray[0] = minmaxArray;
                // Rank 0 receives data from other ranks
                for (int sourceRank = 1; sourceRank < size - 1; sourceRank++) {
                    Status status = MPI.COMM_WORLD.Recv(minmaxArray, 0, 1, MPI.OBJECT, sourceRank, 0);

                    // Process the received data or store it as needed
                    System.out.println("Received from Rank " + sourceRank + ": " + Arrays.toString(minmaxArray));
                    minmaxFavArray[sourceRank] = minmaxArray;
                }

                // print out the minmaxFavArray
                for (int i = 0; i < minmaxFavArray.length; i++) {
                    System.out.println("minmaxFavArray[" + i + "]: " + minmaxArray[i].min + " " + minmaxArray[i].max
                            + " " + minmaxArray[i].i + " " + minmaxArray[i].j);
                }


                for (int sourceRank = size - 1; sourceRank < size; sourceRank++) {
                    Status status = MPI.COMM_WORLD.Recv(minmaxArray, 0, 1, MPI.OBJECT, sourceRank, 0);
                    minmaxUnFavArray[0] = minmaxArray;
                }

                // print out the minmaxUnFavArray
                for (int i = 0; i < minmaxFavArray.length; i++) {
                    System.out.println("minmaxUnFavArray[" + i + "]: " + minmaxArray[i].min + " " + minmaxArray[i].max
                            + " " + minmaxArray[i].i + " " + minmaxArray[i].j);
                }

            } else {
                System.out.println("sending from Rank " + rank + ": " + Arrays.toString(minmaxArray));

                MPI.COMM_WORLD.Send(minmaxArray, 0, 1, MPI.OBJECT, 0, 0);
            }

            // add barrier
            MPI.COMM_WORLD.Barrier();

            if (rank == 0)
            {
                // Final global skyline operation here
                System.out.println("implement final global skyline operation here");
            }

            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;

            if (rank == 0)
                System.out.println("Total Elapsed Time: " + elapsedTime + " milliseconds");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            MPI.Finalize();
        }
    }

    private static double[][][] initializeArray(int numberOfFacilities, int m, int n) throws MPIException {
        double[][][] facilityGrid = new double[numberOfFacilities][m][n];
        java.util.Random rand = new java.util.Random(System.currentTimeMillis());

        for (int facility = 0; facility < numberOfFacilities; facility++) {
            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {

                    facilityGrid[facility][i][j] = 0;

                    if (rand.nextDouble() < 0.1) {
                        facilityGrid[facility][i][j] = 1;
                    }
                }
            }
        }
        return facilityGrid;
    }

    private static double[][][] initializeArray(int facilitiesPerRank, int m, int n, int r, String filePath)
            throws IOException {
        double[][][] facilityGrid = new double[facilitiesPerRank][m][n];

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int facilityIndex;

            while ((line = br.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);

                // Read facility name, row, and grid values
                String facilityName = tokenizer.nextToken();
                int row = Integer.parseInt(tokenizer.nextToken()) - 1; // Adjusted for 0-based indexing

                // Assuming the grid values are in the format "00000000"
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

                // Populate the facilityGrid array
                int belongToRank = facilityIndex / facilitiesPerRank;
                int index = facilityIndex % facilitiesPerRank;

                if (belongToRank == r) {

                    for (int j = 0; j < n; j++) {
                        facilityGrid[index][row][j] = rowValues[j];
                    }
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