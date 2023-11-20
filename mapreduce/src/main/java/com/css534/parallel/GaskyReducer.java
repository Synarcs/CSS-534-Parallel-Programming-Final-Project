package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class GaskyReducer extends MapReduceBase implements Reducer<MapKeys, MapValue, Text, Text> {

    private final String maxRangeValue = "1.7976931348623157E308";
    private Integer gridSize = 0;

    private Vector2FProjections calcBisectorProjections(double x, double y , double x1, double y1){
        Vector2FProjections vector2F = new Vector2FProjections();
        double xx = ((y1 * y1 ) - (y * y) + (x1 * x1) - (x * x)) / 2 * (x1 - x);
        double yy = 0;
        vector2F.setXx(xx); vector2F.setYy(yy);
        return vector2F;
    }

    private Deque<Vector2f> findProximityPoints(List<Vector2f> unDominatedPoints) {
        Deque<Vector2f> intervals = new ArrayDeque<>();
        for (int i=0; i < unDominatedPoints.size(); i+=2){
            Vector2f point1 = unDominatedPoints.get(i-1);
            Vector2f point2 = unDominatedPoints.get(i);
            intervals.add(
                    new Vector2f(
                            (point1.getXx() + point2.getXx()) / 2,
                            0 // point lying on with intersection on X axis
                    )
            );
        }
        return intervals;
    }

    private double findEuclideanDistance(int x, int y, int x1, int y1){
        return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));
    }

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

    private List<Double> mrGaskyAlgorithm(List<Vector2f> cartesianProjectPoints) throws RuntimeException{
        List<Double> distances = new ArrayList<>();

        if (cartesianProjectPoints.size() > 2){
            System.out.println("Implement the further dominance finding algorithm among them");
            List<Vector2f> points = new LinkedList<>();

            for (int i=0; i < cartesianProjectPoints.size(); i++)
                points.add(cartesianProjectPoints.get(i));

            int currentLastVisitedNode = 2;
            int currentWindowStart = 0;
            while (points.size() >= 3 && currentLastVisitedNode <= cartesianProjectPoints.size()){
                Vector2f ii = points.get(0);
                Vector2f jj = points.get(1);
                Vector2f kk = points.get(2);

                if (ii != null && jj != null && kk !=  null){
                    double xij = calcBisectorProjections(ii.getXx(), ii.getYy(), jj.getXx(), jj.getYy()).getXx();
                    double xjk = calcBisectorProjections(jj.getXx(), jj.getYy(), kk.getXx(), kk.getYy()).getXx();

                    if (xij > xjk){
                        points.remove(1);
                        currentLastVisitedNode++; // move the window to the right
                    }else {
                        currentWindowStart++;
                        currentLastVisitedNode++;
                    }
                }
            }
            cartesianProjectPoints = cartesianProjectPoints;
            return distances;
        }
        // start the proximity interval calculations assume the algorithm is applied
        Deque<Vector2f> proximityIntervalPoints = findProximityPoints(cartesianProjectPoints);
        proximityIntervalPoints.size();

        // TODO: Need to implement the proximity distance call for all the set of the points in the grid
        return distances;
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
        List<Double> distances = new ArrayList<>();
        try {
            mrGaskyAlgorithm(cartesianProjections);
        }catch (RuntimeException exception){

        }

        StringBuilder totalDistances = new StringBuilder();
        for (Map.Entry<Integer, Double> val: orderedMap){
            totalDistances.append("(" + String.valueOf(val.getKey()) + " " + Double.valueOf(val.getValue()) + ")");
            totalDistances.append(" ");
        }

        outputCollector.collect(
                new Text(String.valueOf(mapKeys.getFeatureName() + " " + mapKeys.getColValue())),
                new Text(totalDistances.toString())
        );
    }
}
