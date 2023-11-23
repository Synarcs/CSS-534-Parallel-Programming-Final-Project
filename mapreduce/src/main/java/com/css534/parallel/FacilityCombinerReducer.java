package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;


public class FacilityCombinerReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<MapKeys, MapValue, Text, Text> {

    @Override
    public void reduce(MapKeys mapKeys, Iterator<MapValue> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
      // implement the multi skyline feature combiner implementation
    }
}