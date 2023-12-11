package com.css534.parallel;

import mpi.*;
import java.io.BufferedReader;
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

public class MRGASKYMPI {

    public static void main(String[] args) throws MPIException {
        MPI.Init(args);

        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        if (rank == 0 && args.length != 5) {
            System.err.println("Usage: java MRGASKYMPI numberOfFacilities size size favourable unfavourable");
            MPI.Finalize();
            System.exit(1);
        }

        // start timer
        long startTime = System.currentTimeMillis();

        int numberOfFacilities = Integer.parseInt(args[0]);
        int m = Integer.parseInt(args[1]);
        int n = Integer.parseInt(args[2]);
        int Fav = Integer.parseInt(args[3]); // Number of favorable facilities
        int Unfav = Integer.parseInt(args[4]); // Number of unfavorable facilities

        // Calculate the number of ranks that will receive favorable and unfavorable
        // facilities
        int numRanksForF = size / 2;
        int numRanksForU = size - numRanksForF;

        // Arrays to store the distributed facilities for each rank
        int[] distributedF = new int[numRanksForF];
        int[] distributedU = new int[numRanksForU];

        // Calculate how many favorable facilities each rank in the first half will
        // receive
        int facilitiesPerRankForF = Fav / numRanksForF;
        // Calculate the remainder of favorable facilities
        int remainderF = Fav % numRanksForF;

        // Distribute favorable facilities
        for (int i = 0; i < numRanksForF; i++) {
            distributedF[i] = facilitiesPerRankForF + (i < remainderF ? 1 : 0);
        }

        // Calculate how many unfavorable facilities each rank in the second half will
        // receive
        int facilitiesPerRankForU = Unfav / numRanksForU;
        // Calculate the remainder of unfavorable facilities
        int remainderU = Unfav % numRanksForU;

        // Distribute unfavorable facilities
        for (int i = 0; i < numRanksForU; i++) {
            distributedU[i] = facilitiesPerRankForU + (i < remainderU ? 1 : 0);
        }

        // Gather the distributed facilities to all ranks
        int[] allDistributedF = new int[numRanksForF * size];
        int[] allDistributedU = new int[numRanksForU * size];

        // Gather the rank distributed info on rank-0
        MPI.COMM_WORLD.Gather(distributedF, 0, numRanksForF, MPI.INT, allDistributedF, 0, numRanksForF, MPI.INT, 0);
        MPI.COMM_WORLD.Gather(distributedU, 0, numRanksForU, MPI.INT, allDistributedU, 0, numRanksForU, MPI.INT, 0);

        // Print the result for understnading the distribution
        if (rank == 0) {
            System.out.println("Rank  |  Distributed Favorable Facilities");
            System.out.println("----------------------------------------");
            for (int i = 0; i < numRanksForF; i++) {
                System.out.println("  " + i + "        " + allDistributedF[i]);
            }
            System.out.println("----------------------------------------");

            System.out.println("Rank  |  Distributed Unfavorable Facilities");
            System.out.println("----------------------------------------");
            for (int i = 0; i < numRanksForU; i++) {
                System.out.println("  " + (i + numRanksForF) + "        " + allDistributedU[i]);
            }
            System.out.println("----------------------------------------");
        }

        // Calculate the number of facilities and offset for this rank
        // This will distribute facility grids to different rank based on count and type
        int facilities = 0;
        int offset = 1;
        if (rank < numRanksForF) {
            facilities = distributedF[rank];
            for (int i = 0; i < rank; i++) {
                offset += distributedF[i];
            }
        } else {
            facilities = distributedU[rank - numRanksForF];
            for (int i = 0; i < numRanksForF; i++) {
                offset += distributedF[i];
            }
            for (int i = 0; i < rank - numRanksForF; i++) {
                offset += distributedU[i];
            }
        }

        try {
            // Each facility will process assigned facilities
            String filePath = "input.txt";
            double[][][] FacilityGrid = new double[facilities][m][n];
            FacilityGrid = initializeArray(facilities, offset, m, n, rank, filePath);

            // MRGASKY algorithm
            // Step -1 calculate distance to closest facility in the same row
            for (int facility = 0; facility < facilities; facility++) {
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
            // Calculate the Eucledian distance to nearest facility - column wise
            for (int facility = 0; facility < facilities; facility++) {
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

                    // Iterate over cartesianProjections and filter the points which have infinity as y-coordinate
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

            // Step-3 - Apply Min-max algorithm to local skyline obejcts
            Minmax[] minmaxArray = new Minmax[m * n];
            int index = 0;

            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    Double local_min_val = Double.MAX_VALUE;
                    Double local_max_val = Double.MIN_VALUE;
                    Minmax minmax = new Minmax();
                    for (int k = 0; k < facilities; k++) {
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

            // Let all processes complete local skyline computation
            MPI.COMM_WORLD.Barrier();

            // Gather the minmaxArray to rank 0
            Minmax[] allMinmaxArray = null;
            if (rank == 0) {
                allMinmaxArray = new Minmax[m * n * size];
            }

            // Gather min-max distances from all the ranks to rank-0
            MPI.COMM_WORLD.Gather(minmaxArray, 0, m * n, MPI.OBJECT, allMinmaxArray, 0, m * n, MPI.OBJECT, 0);

            // Print the result on rank 0
            if (rank == 0) {
                // get the non-zero fav and unfav ranks count
                long favRankCount = java.util.Arrays.stream(distributedF).filter(value -> value != 0).count();
                long unfavRankCount = java.util.Arrays.stream(distributedU).filter(value -> value != 0).count();

                // based on allDistributedU array
                Minmax[][] minmaxFavArray = new Minmax[(int) favRankCount][m * n];
                Minmax[][] minmaxUnFavArray = new Minmax[(int) unfavRankCount][m * n];

                // create favminmaxArray and unfavminmaxArray index to store favorable and
                // unfavorable facilities
                int favminmaxArrayIndex = 0;
                int unfavminmaxArrayIndex = 0;

                // Iterate over allMinmaxArray and store favorable and unfavorable facilities
                for (int i = 0; i < allMinmaxArray.length; i++) {
                    if (i < (favRankCount * m * n)) {
                        minmaxFavArray[favminmaxArrayIndex / (m * n)][favminmaxArrayIndex
                                % (m * n)] = allMinmaxArray[i];
                        favminmaxArrayIndex++;
                    } else if (i >= (favRankCount * m * n) && i < (favRankCount * m * n) + (unfavRankCount * m * n)) {
                        minmaxUnFavArray[unfavminmaxArrayIndex / (m * n)][unfavminmaxArrayIndex
                                % (m * n)] = allMinmaxArray[i];
                        unfavminmaxArrayIndex++;
                    }
                }

                List<Minmax> globalFavFlattened = new ArrayList<>();
                List<Minmax> globalUnFavFlattened = new ArrayList<>();

                // Apply same min-max algorithm to calculate Global skyline objects
                // first, process Favorable facilities
                for (int j = 0; j < m * n; j++) {
                    double globalMaxIndexFav = Double.MIN_VALUE;
                    double globalMinimaIndexFav = Double.MAX_VALUE;
                    int row = 0;
                    int column = 0;

                    for (int favFacilityCount = 0; favFacilityCount < minmaxFavArray.length; favFacilityCount++)
                        globalMaxIndexFav = Double.min(globalMaxIndexFav, minmaxFavArray[favFacilityCount][j].max);
                    globalMinimaIndexFav = Double.max(globalMinimaIndexFav,
                            minmaxFavArray[favFacilityCount][j].min);
                    row = minmaxFavArray[favFacilityCount][j].i;
                    column = minmaxFavArray[favFacilityCount][j].j;
                }
                    globalFavFlattened.add(
                            new Minmax(globalMinimaIndexFav, globalMaxIndexFav, row, column));

                }

                // process Unfavorable facilities
                for (int j = 0; j < m * n; j++) {
                    double globalMaxIndexUnFav = Double.MIN_VALUE;
                    double globalMinimaIndexUnFav = Double.MAX_VALUE;
                    int row = 0;
                    int column = 0;

                    if (unfavRankCount == 1) {
                        globalMaxIndexUnFav = minmaxUnFavArray[0][j].min;
                        globalMinimaIndexUnFav = minmaxUnFavArray[0][j].max;
                        row = minmaxFavArray[0][j].i;
                        column = minmaxFavArray[0][j].j;
                    } else {

                        for (int unfavFacilityCount = 0; unfavFacilityCount < minmaxUnFavArray.length; unfavFacilityCount++) {
                            globalMaxIndexUnFav = Double.min(globalMaxIndexUnFav,
                                    minmaxUnFavArray[unfavFacilityCount][j].max);
                            globalMinimaIndexUnFav = Double.max(globalMinimaIndexUnFav,
                                    minmaxUnFavArray[unfavFacilityCount][j].min);
                            row = minmaxFavArray[unfavFacilityCount][j].i;
                            column = minmaxFavArray[unfavFacilityCount][j].j;
                        }
                    }
                    globalUnFavFlattened.add(
                            new Minmax(globalMaxIndexUnFav, globalMinimaIndexUnFav, row, column));
                }

                assert globalFavFlattened.size() == globalUnFavFlattened.size();

                for (int i = 0; i < m * n; i++) {
                    double globalMaxIndexFav = globalFavFlattened.get(i).max;
                    double globalMinimaIndexFav = globalFavFlattened.get(i).min;
                    int favi = globalFavFlattened.get(i).i + 1;
                    int favj = globalFavFlattened.get(i).j + 1;

                    double globalMinimaIndexUnFav = globalUnFavFlattened.get(i).min;
                    double globalMaxIndexUnFav = globalUnFavFlattened.get(i).max;

                    if (globalMinimaIndexFav != Double.MAX_VALUE && globalMinimaIndexUnFav != Double.MAX_VALUE) {
                        if (globalMinimaIndexUnFav < globalMinimaIndexFav
                                || globalMinimaIndexFav == globalMinimaIndexUnFav ||
                                globalMaxIndexFav > globalMinimaIndexUnFav
                                || globalMinimaIndexFav > globalMaxIndexUnFav) {

                            continue;
                        } else {
                            System.out.println(favi + " " + favj);
                        }
                    }
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

    // Initialize the ranks with facility grids
    private static double[][][] initializeArray(int facility, int offset, int m, int n, int rank, String filePath)
            throws IOException {
        if (facility > 0) {
            double[][][] facilityGrid = new double[facility][m][n];

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
                        facilityIndex = Integer.parseInt(facilityName.substring(1, 2));

                    } else {
                        facilityIndex = Integer.parseInt(facilityName.substring(1));
                    }

                    if (offset <= facilityIndex && facilityIndex < offset + facility) {
                        for (int j = 0; j < n; j++) {
                            facilityGrid[facilityIndex - offset][row][j] = rowValues[j];
                        }
                    }
                }
            }
            return facilityGrid;

        } else {
            return null;
        }
    }

    //region MRGASKYMPI Classes and helpers
    public static class Minmax implements Serializable {
        double min;
        double max;
        int i;
        int j;

        Minmax() {
        };

        Minmax(double min, double max, int i, int j) {
            this.min = min;
            this.max = max;
            this.i = i;
            this.j = j;
        }
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

    // GASKY algorithm to calculate dominant points using voronoi polygons(Local skyline objects)
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

    // Get rows with distances in the order
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
    //endregion

}
