package com.css534.parallel;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

public class GaskyReducer extends MapReduceBase implements Reducer<MapKeys, DoubleWritable, Text, Text> {

    private double calcBisector(int x, int y , int x1, int y1){
        return ((y1 * y1 ) - (y * y) + (x1 * x1) - (x * x)) / 2 * (x1 - x);
    }


    public void mrGaskyAlgorithm(){}

    @Override
    public void reduce(MapKeys mapKeys, Iterator<DoubleWritable> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        /*
                Each will get format
                this gets all the values sorted based on their column values
        */
        Stack<NodeVectors> values = new Stack<>();

        int nodeRows = 1;
        // generate init stack
        while (iterator.hasNext()){
            values.push(
                    new NodeVectors(
                            nodeRows,
                            iterator.next().get()
                    )
            );
        }

        while (!values.isEmpty()){
            NodeVectors vector = values.pop();
            outputCollector.collect(
                    new Text(String.valueOf(vector.getRow())),
                    new Text(String.valueOf(vector.getDistance()))
            );
        }

    }

}
