package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;


public class FacilityCombinerReducer extends MapReduceBase implements Reducer<Text, Vector2f, Text, Text> {

    @Override
    public void reduce(Text facilityName, Iterator<Vector2f> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        Set<Vector2f> uniqueLocalSkylineObjects = new HashSet<>();

        // O(nlogn)
        while (iterator.hasNext()){
            uniqueLocalSkylineObjects.add(
                    iterator.next()
            );
        }

        StringBuilder builder = new StringBuilder();

        uniqueLocalSkylineObjects.stream().forEach((value) -> {
            builder.append(value.getXx() + "," + value.getYy());
            builder.append(",");
        });

        String data = builder.substring(0, builder.length() - 1);

        outputCollector.collect(
                new Text(facilityName),
                new Text(data)
        );
    }
}