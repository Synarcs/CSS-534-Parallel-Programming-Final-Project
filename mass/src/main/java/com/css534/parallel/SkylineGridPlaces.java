package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Place;
import edu.uw.bothell.css.dsl.MASS.logging.Log4J2Logger;

import java.io.FileInputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.css534.parallel.GridConstants.INIT;
import static com.css534.parallel.GridConstants.INIT_BINARY_MATRIX;
import static com.css534.parallel.GridConstants.COMPUTE_BEST_ROW_DISTANCE;
import static com.css534.parallel.GridConstants.COMPUTE_PROXIMITY_POLYGONS;
import static com.css534.parallel.GridConstants.COMPUTE_GLOBAL_SKYLINE;
import static com.css534.parallel.GridConstants.FAVOURABLE;
import static com.css534.parallel.GridConstants.UNFAVOURABLE;


@SuppressWarnings("unused")
public class SkylineGridPlaces extends Place {
    // file object to be read by all the places file shared across all the computing
    // nodes
    public static FileInputStream file = null;
    public static String filename = null;
    private static boolean debug = false;

    public SkylineGridPlaces(Object object) {
        filename = (String) object;
    }

    public SkylineGridPlaces() {
        super();
    }

    private double[][] facilityGridPlaces;
    private double[][] distanceGrid;
    private boolean isFavourableFacility;
    private String facilityType;
    private int favCount;
    private int unFavCount;
    private int gridX = -1;
    private int gridY = -1;

    private Log4J2Logger getLogger(){
        return  MASS.getLogger();
    }

    private double[] getLeftDistance(double[] gridRows) {
        boolean isFavlFound = false;
        double[] leftDistance = new double[gridRows.length];
        Arrays.fill(leftDistance, Double.MAX_VALUE);
        for (int i = 0; i < gridRows.length; i++) {
            if (gridRows[i] == 1) {
                // a facility one
                leftDistance[i] = 0;
                isFavlFound = true;
            } else {
                if (isFavlFound) {
                    leftDistance[i] = leftDistance[i - 1] + 1;
                }
            }
        }
        return leftDistance;
    }

    private double[] getRightDistance(double[] gridRows) {
        boolean isFavrFound = false;
        double[] rightDistance = new double[gridRows.length];
        Arrays.fill(rightDistance, Double.MAX_VALUE);

        for (int i = gridRows.length - 1; i >= 0; --i) {
            if (gridRows[i] == 1) {
                rightDistance[i] = 0;
                isFavrFound = true;
            } else {
                if (isFavrFound) {
                    rightDistance[i] = rightDistance[i + 1] + 1;
                }
            }
        }
        return rightDistance;
    }

    public Object init(Object facilityName) {
        int totalFacilities = getSize()[0];
        int currentFacility = getIndex()[0] + 1;

        int[] gridSizePerFacility = (int[]) facilityName;


        // create the final holder pplace to load all the arrays to the heap for the
        // place thread
        facilityGridPlaces = new double[gridSizePerFacility[0]][gridSizePerFacility[1]];
        gridX = gridSizePerFacility[0];
        gridY = gridSizePerFacility[1];
        // favCount = gridSizePerFacility[2];
        // unFavCount = gridSizePerFacility[3];
        // if (currentFacility <= favCount) facilityType = FAVOURABLE;
        // else facilityType = UNFAVOURABLE;

        return null;
    }

    public void computeBestAgentDistance() {

        int distanceRowIndex = 0;
        distanceGrid = new double[gridX][gridY];

        for (double[] row : facilityGridPlaces) {
            double[] leftDistance = getLeftDistance(row);
            double[] rightDistance = getRightDistance(row);

            for (int k = 0; k < gridY; k++) {
                distanceGrid[distanceRowIndex][k] = Double.min(
                        leftDistance[k], rightDistance[k]);
            }
            distanceRowIndex++;
        }

        if (debug) {
            for (int i = 0; i < gridX; i++) {
                getLogger().debug(Arrays.toString(distanceGrid[i]));
            }
        }
    }

    private void computeProximityPolygons() {
        //System.out.println("Computing proximity polygons for place " + getIndex()[0]);
        for (int column = 0; column < gridY; column++) {

            // System.out.println("Started Computing proximity polygons for place " + getIndex()[0] + " and column "
            // + column);
            List<Map.Entry<Integer, Double>> orderedMap = getOrderedRowValues(column, gridX, distanceGrid);

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
            SkylineObjects skylineObject = mrGaskyAlgorithm(cartesianProjections, gridSize);

            // System.out.println("Skyline Objects for places " + getIndex()[0] + " and column " + column);
            if (debug){
                for (Vector2f vector : skylineObject.getSkylineObjects()) {
                    getLogger().debug(vector.getXx() + " " + vector.getYy());
                }
            }


            for (int i = 0; i < skylineObject.getDistances().size(); i++) {
                distanceGrid[i][column] = skylineObject.getDistances().get(i);
            }
        }

        if (debug){
            getLogger().debug("Distance Grid for places " + getIndex()[0]);
        }

        for (int i = 0; i < distanceGrid.length; i++) {
            getLogger().debug(Arrays.toString(distanceGrid[i]));
        }

    }

    public double loadDistance(Object argument) {

        //System.out.println(argument.getClass().getName() + argument.getClass().getCanonicalName());
        Object[] indexAskedByAgent = (Object[]) argument;
        int[] intArray = Arrays.stream(indexAskedByAgent)
                .mapToInt(obj -> (int) obj)
                .toArray();
        //System.out.println("the arguments are" + Arrays.toString(intArray));
        try {
            if (distanceGrid == null)
                throw new RuntimeException("Eror please call agent call before");
            //System.out.println("from the place " + " " + getIndex()[0] + " " + getSize()[0]);
            return distanceGrid[intArray[0]][intArray[1]];
        } catch (RuntimeException exception) {
            exception.printStackTrace();
            return Double.MAX_VALUE;
        }
    }

    public void loadBinaryMatrix(Object argument) {

        String[] tokens = (String[]) argument;
        // System.out.println("Binary tokens at this place" + Arrays.toString(tokens) + " " + getIndex()[0]);

        for (int i = 0; i < tokens.length; i++) {
            for (int j = 0; j < tokens[0].length(); j++) {
                facilityGridPlaces[i][j] = Double.valueOf(
                        Character.getNumericValue(tokens[i].charAt(j)));
            }
        }

        // TODO: chage everything to use mass debug log
        if (debug) {
            getLogger().debug("for place id " + getIndex()[0]);

            for (int i = 0; i < facilityGridPlaces.length; i++) {
                getLogger().debug(Arrays.toString(facilityGridPlaces[i]));
            }
        }

        return;
    }

    // #region Skyline Algorithm
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

    private static List<Map.Entry<Integer, Double>> getOrderedRowValues(int columnIndex, int m,
                                                                        double[][] FacilityGrid) {
        List<Map.Entry<Integer, Double>> orderedMap = new ArrayList<>();
        // Iterate over the specified portion of FacilityGrid
        for (int row = 0; row < m; row++) {
            double distance = FacilityGrid[row][columnIndex];
            orderedMap.add(new AbstractMap.SimpleEntry<>(row + 1, distance));
        }

        return orderedMap;
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

    private static Vector2FProjections calcBisectorProjections(double x, double y, double x1, double y1) {

        double xx = ((y1 * y1) - (y * y) + (x1 * x1) - (x * x)) / (2 * (x1 - x));
        double yy = 0;

        Vector2FProjections vector2F = new Vector2FProjections(xx, yy);

        return vector2F;
    }

    @Override
    public Object callMethod(int functionId, Object argument) {
        switch (functionId) {
            case INIT: {
                init(argument);
                break;
            }
            case INIT_BINARY_MATRIX: {
                loadBinaryMatrix(argument);
                break;
            }
            case COMPUTE_BEST_ROW_DISTANCE: {
                computeBestAgentDistance();
                break;
            }
            case COMPUTE_PROXIMITY_POLYGONS: {
                computeProximityPolygons();
                break;
            }
            case COMPUTE_GLOBAL_SKYLINE: {
                return loadDistance(argument);
            }
            default: { break; }
        }
        return null;
    }

}
