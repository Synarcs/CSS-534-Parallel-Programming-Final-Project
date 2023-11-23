package com.css534.parallel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class FacilityMapper extends MapReduceBase implements Mapper<LongWritable, Text, MapKeys, MapValue> {

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<MapKeys, MapValue> outputCollector, Reporter reporter) throws IOException {
        // implement the combined facility mapper for all features
    }
}
