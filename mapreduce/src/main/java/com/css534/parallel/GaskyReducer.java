package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

public class GaskyReducer extends MapReduceBase implements Reducer<MapKeys, Text, Text, Text> {


    @Override
    public void reduce(MapKeys mapKeys, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

    }
}
