package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import javax.sound.sampled.Line;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class GaskyReducer extends MapReduceBase implements Reducer<MapKeys, MapValue, Text, Text> {

    private final String maxRangeValue = "1.7976931348623157E308";
    private Integer gridSize = 0;

    private Log log = LogFactory.getLog(GaskyReducer.class);

    @Override
    public void configure(JobConf job) {
        // TODO Auto-generated method stub
        super.configure(job);
    }

    private Vector2FProjections calcBisectorProjections(double x, double y , double x1, double y1){
        Vector2FProjections vector2F = new Vector2FProjections();
        double xx = ((y1 * y1 ) - (y * y) + (x1 * x1) - (x * x)) / 2 * (x1 - x);
        double yy = 0;
        vector2F.setXx(xx); vector2F.setYy(yy);
        return vector2F;
    }

    private List<double[]> findProximityPoints(List<Vector2f> unDominatedPoints) {
        List<Vector2f> intervals = new ArrayList<>();
        for (int i=1; i < unDominatedPoints.size(); i++){
            Vector2f point1 = unDominatedPoints.get(i-1);
            Vector2f point2 = unDominatedPoints.get(i);
            intervals.add(
                    new Vector2f(
                            (point1.getXx() + point2.getXx()) / 2,
                            0 // point lying on with intersection on X axis
                    )
            );
        }
        // i can do it in O(1) space, lazy to do it lol
        // implementation of combine intervals using a the interval frame
        List<double[] > mergedInterval = new ArrayList<>(intervals.size());
        mergedInterval.add(
                new double[]{1, intervals.get(0).getXx()}
        );
        for (int i = 1; i < intervals.size(); i++){
            mergedInterval.add(
                    new double[]{intervals.get(i - 1).getXx(), intervals.get(i).getXx()}
            );
        }
        mergedInterval.add(
                new double[]{intervals.get(intervals.size() - 1).getXx(), gridSize}
        );
        return mergedInterval;
    }

    public Deque<Vector2f> findProximityPointsSingle(List<Vector2f> unDominatedPoints){
        Deque<Vector2f> intervals = new LinkedList<>();
        for (int i=1; i < unDominatedPoints.size(); i++){
            Vector2f point1 = unDominatedPoints.get(i-1);
            Vector2f point2 = unDominatedPoints.get(i);
            intervals.add(
                    new Vector2f(
                            (point1.getXx() + point2.getXx()) / 2,
                            0 // point lying on with intersection on X axis
                    )
            );
        }
        return  intervals;
    }

    private double findEuclideanDistance(int x, int y, int x1, int y1){return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));}

    private double findEuclideanDistance(double x, double y, double x1, double y1){return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));}

    private List<Map.Entry<Integer, Double>> getOrderedRowValues(Iterator<MapValue> valueIterator){
        // O(nlogn)
        Map<Integer, Double> orderedMap = new HashMap<>();
        while (valueIterator.hasNext()) {
            MapValue iterator = valueIterator.next();
            orderedMap.put((int)iterator.getRowValue(), iterator.getDistance());
        }
        return orderedMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());
    }

    /*
        This will run for each column and determine the closest distance from skyline objects
        This will all return the final skyline objects if any
     */
    private SkylineObjects mrGaskyAlgorithm(List<Vector2f> cartesianProjectPoints, Integer colNumber) throws RuntimeException, NoSuchElementException {
        int totalPoints = cartesianProjectPoints.size();
        List<Double> distances = new ArrayList<>(Collections.nCopies(gridSize, Double.MAX_VALUE));

        if (totalPoints > 2) {
            log.info("The current length of the un dominated grids is" + cartesianProjectPoints.size());
            log.info("This is a non implemented method yet");

            List<Vector2f> points = new LinkedList<>();

            for (int i = 0; i < cartesianProjectPoints.size(); i++)
                points.add(cartesianProjectPoints.get(i));

            // filtering the points based on the dominance to further calculate proximity distance
            int currentWindowStart = 1;
            while (points.size() >= 3 && currentWindowStart <= points.size() - 2) {
                Vector2f ii = points.get(currentWindowStart - 1);
                Vector2f jj = points.get(currentWindowStart);
                Vector2f kk = points.get(currentWindowStart + 1);

                if (ii != null && jj != null && kk != null) {
                    double xij = calcBisectorProjections(ii.getXx(), ii.getYy(), jj.getXx(), jj.getYy()).getXx();
                    double xjk = calcBisectorProjections(jj.getXx(), jj.getYy(), kk.getXx(), kk.getYy()).getXx();

                    if (xij > xjk) {
                        points.remove(currentWindowStart);
                        currentWindowStart++;
                    } else {
                        currentWindowStart++;
                    }
                }
            }

            log.info("The current remained dominated points are");
            List<double[]> proximityProjectionsPoints = findProximityPoints(points);
            List<Double> testData = proximityProjectionsPoints.stream().map((i) -> i[0]).collect(Collectors.toList());

            int unDominatedPointsSize = points.size();
            int proximityIntervals = proximityProjectionsPoints.size() - 1;
            // // this will always be greater than 2 (since for this case we alwasy have more than 2 cartesian points in the grid).
            assert  unDominatedPointsSize == proximityIntervals;
            int dominatedCoordinatesDistances = 0;
//          [ [1, 3.0] [3, 5.0] [5, 8]] || (2.0,3.0)(4.0,1.0)(6.0,6.0)

            for (int interval=0; interval < proximityProjectionsPoints.size(); interval++){
                double[] currentInterval = proximityProjectionsPoints.get(interval);
                int start = (int) currentInterval[0]; int end = (int) currentInterval[1];
                Vector2f dominantPoint = points.get(dominatedCoordinatesDistances);
                // we only consider the int projection over x asix for grid skyline
                for (int xCord=start; xCord <= end; xCord++){
                    if (distances.get(xCord - 1) != Double.MAX_VALUE){
                        distances.set(
                                xCord - 1,
                                Double.min(
                                        findEuclideanDistance(xCord, 0, dominantPoint.getXx(), dominantPoint.getYy()),
                                        distances.get(xCord - 1)
                                )
                        );
                    }else
                        distances.set(
                                xCord - 1,
                                findEuclideanDistance(xCord, 0, dominantPoint.getXx(), dominantPoint.getYy())
                        );
                }
                dominatedCoordinatesDistances++;
            }

            for (Vector2f point: points){
                double y = point.getYy();
                point.setXx(y); // recasting over the plane of grid that was initially projected over the plane
                point.setYy(colNumber);
            }

            // check for the points based on the dominance
            return new SkylineObjects(
                    distances,
                    points
            );
        }

        Deque<Vector2f> proximityProjectionsPoints = findProximityPointsSingle(cartesianProjectPoints);
        if (proximityProjectionsPoints.size() == 0 && cartesianProjectPoints.size() == 1){
            // only one dominant point exist hence has no partitions present
            for (int i = 1; i <= gridSize; i++){
                distances.set(i-1,
                        findEuclideanDistance(i, 0, cartesianProjectPoints.get(0).getXx(), cartesianProjectPoints.get(0).getYy())
                );
            }
            return new SkylineObjects(
                    distances,
                    cartesianProjectPoints
            );
        }
        else if (proximityProjectionsPoints.size() == 0 && cartesianProjectPoints.size() == 0) {
            // nothing is present all are dominated by each other
            double[] maxDistance = new double[gridSize];
            Arrays.fill(maxDistance, Double.MAX_VALUE);
            return new SkylineObjects(
                    Arrays.stream(maxDistance).boxed().collect(Collectors.toList()),
                    new ArrayList<>()
            );
        }

        /*
            it has 2 dominated points in the grid and single interval that is bisector of dominated coordinates
            It is not possible to overlap over each other since the projection of these points is over cartesian x asis grid
        */
        Vector2f intervalProjection = proximityProjectionsPoints.removeFirst();
        for (int i=1; i <= gridSize ; i++) {
            if (i ==  intervalProjection.getXx()){
                distances.set(
                        i - 1,
                        Double.min(
                                findEuclideanDistance(i, 0, cartesianProjectPoints.get(0).getXx(), cartesianProjectPoints.get(0).getYy()),
                                findEuclideanDistance(cartesianProjectPoints.get(1).getXx(), cartesianProjectPoints.get(1).getYy(), i, 0)
                        )
                );
            }else if (i < intervalProjection.getXx()){
                distances.set(i - 1,
                        findEuclideanDistance(i, 0, cartesianProjectPoints.get(0).getXx(), cartesianProjectPoints.get(0).getYy())
                );
            }else if (i > intervalProjection.getXx()){
                distances.set(i - 1,
                        findEuclideanDistance(cartesianProjectPoints.get(1).getXx(), cartesianProjectPoints.get(1).getYy(), i, 0)
                );
            }
        }
        return new SkylineObjects(
                distances, cartesianProjectPoints
        );
    }


    @Override
    public void reduce(MapKeys mapKeys, Iterator<MapValue> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        /*
                Each will get format
                this gets all the values sorted based on their column values
        */
        List<Map.Entry<Integer, Double>> orderedMap = getOrderedRowValues(iterator);
        gridSize = orderedMap.size();
        /*
                Cast the projection of these points onto the cartesian grid in the form of Vector2f
         */
        List<Vector2f> cartesianProjections = orderedMap.stream().map((Map.Entry<Integer, Double> value) -> {
            return new Vector2f(
                    value.getKey(),
                    value.getValue()
            );
        }).collect(Collectors.toList());

        /*
               Filter out the highly unfavourable facilities that are too far from the desired system
         */
        cartesianProjections = cartesianProjections.stream().filter((Vector2f vector) -> {
            return vector.getYy() != Double.MAX_VALUE;
        }).collect(Collectors.toList());

        /*
            The final result of the reduce function
            Will hold all the required euclidean distance from the un dominated points to cartesian x axis grid
            All distance must be based on the proximity interval barrier across all set of points
        */
        SkylineObjects objects = mrGaskyAlgorithm(cartesianProjections, mapKeys.getColValue());

        StringBuilder totalDistances = new StringBuilder();

        for (int i=0; i < objects.getDistances().size(); i++){
            totalDistances.append(objects.getDistances().get(i));
            totalDistances.append(" ");
        }
        totalDistances.append("|| ");

        int objectSize = objects.getSkylineObjects().size();
        totalDistances.append("[");
        for (int i=0; i < objectSize; i++){
            totalDistances.append(
                    objects.getSkylineObjects().get(i).getXx() + "," + objects.getSkylineObjects().get(i).getYy()
            );
            if (i != objectSize - 1)
                totalDistances.append(",");
        }
        totalDistances.append("]");

        outputCollector.collect(
                new Text(String.valueOf(mapKeys.getFeatureName() + " " + mapKeys.getColValue())),
                new Text(totalDistances.toString())
        );
    }
}
