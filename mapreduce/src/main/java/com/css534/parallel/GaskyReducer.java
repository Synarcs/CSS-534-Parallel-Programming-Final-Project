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

    private Vector2FProjections calcBisector(int x, int y , int x1, int y1){
        Vector2FProjections vector2F = new Vector2FProjections();
        double xx = ((y1 * y1 ) - (y * y) + (x1 * x1) - (x * x)) / 2 * (x1 - x);
        double yy = 0;
        vector2F.setXx(xx); vector2F.setYy(yy);
        return vector2F;
    }


    private Text keyText = new Text();
    public void mrGaskyAlgorithm(){}

    private int findProximityDistance(){
        return 0;
    }

    private int findProximityIntervalPoint(){ return 0; }

    private double findEuclideanDistance(int x, int y, int x1, int y1){
        return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));
    }

    public List<Double> getOrderedRowValues(Iterator<MapValue> valueIterator){
        Map<Integer, Double> orderRowColumns = new HashMap<>();
        while (valueIterator.hasNext()){
            orderRowColumns.put(valueIterator.next().getRowValue(),
                    orderRowColumns.getOrDefault(valueIterator.next().getDistance(), 0.0)+1);
        }
        return orderRowColumns.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map((Map.Entry<Integer, Double> entry) -> entry.getValue())
                .collect(Collectors.toList());
    }

    @Override
    public void reduce(MapKeys mapKeys, Iterator<MapValue> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        /*
                Each will get format
                this gets all the values sorted based on their column values
        */
        Stack<Vector2FProjections> values = new Stack<>();
        List<Double> orderedRowValues = getOrderedRowValues(iterator);

        int nodeRows = 1;
        // generate init stack
        while (iterator.hasNext()){
            values.push(
                    new Vector2FProjections(
                            nodeRows,
                            orderedRowValues.get(nodeRows - 1)
                    )
            );
            nodeRows++;;
        }


        StringBuilder totalDistances = new StringBuilder();

        for (Double data : orderedRowValues){
            totalDistances.append(data);
            totalDistances.append(" ");
        }

        outputCollector.collect(
                new Text(String.valueOf(mapKeys.getFeatureName() + " " + mapKeys.getColValue())),
                new Text(totalDistances.toString())
        );
    }

}
