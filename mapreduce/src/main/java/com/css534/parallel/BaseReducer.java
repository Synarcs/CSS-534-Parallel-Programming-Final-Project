package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/*
    Class Only used to verify the result combination of the mapper action over the data grouped over column value
 */
public class BaseReducer extends MapReduceBase implements Reducer<MapKeys, MapValue, Text, Text> {

    private final String maxRangeValue = "1.7976931348623157E308";
    private Vector2FProjections calcBisector(int x, int y , int x1, int y1){
        Vector2FProjections vector2F = new Vector2FProjections();
        double xx = ((y1 * y1 ) - (y * y) + (x1 * x1) - (x * x)) / 2 * (x1 - x);
        double yy = 0;
        vector2F.setXx(xx); vector2F.setYy(yy);
        return vector2F;
    }


    private Text keyText = new Text();
    public void mrGaskyAlgorithm(){}

    private int findProximityDistance(Double x1, Double y1, Double x2, Double y2){
        return 0;
    }

    private int findProximityIntervalPoint(){ return 0; }

    private double findEuclideanDistance(int x, int y, int x1, int y1){
        return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));
    }

    public List<Map.Entry<Integer, Double>> getOrderedRowValues(Iterator<MapValue> valueIterator){
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

    @Override
    public void reduce(MapKeys mapKeys, Iterator<MapValue> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        /*
                Each will get format
                this gets all the values sorted based on their column values
        */
        Stack<Vector2FProjections> vactor2dProjections = new Stack<>();
        List<Map.Entry<Integer, Double>> orderedMap = getOrderedRowValues(iterator);


        // List<Double> proximDistance = orderedMap.stream().map((value) -> value.getValue()).collect(Collectors.toList());

        List<Vector2f> cartesianProjections = orderedMap.stream().map((value) -> {
            return new Vector2f(
                    value.getKey(),
                    value.getValue()
            );
        }).collect(Collectors.toList());


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
