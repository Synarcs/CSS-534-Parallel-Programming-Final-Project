package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

public class FacilityCombinerReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text facilityName, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        Set<Vector2f> uniqueLocalSkylineObjects = new HashSet<>();
        StringBuilder ans = new StringBuilder();
        // // O(nlogn)
        while (iterator.hasNext()){
            String skylineObjectPayload = iterator.next().toString();
            skylineObjectPayload = skylineObjectPayload.trim().replace("[", "").replace("]", "").trim();
            double[] objectPayload = Arrays.stream(skylineObjectPayload.split(","))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

            for (int i = 0; i < objectPayload.length; i+=2 ){
                uniqueLocalSkylineObjects.add(
                        new Vector2f(
                                objectPayload[i], objectPayload[i+1]
                        )
                );
            }
        }

        StringBuilder builder = new StringBuilder();

        uniqueLocalSkylineObjects.stream().forEach((value) -> {
            builder.append(value.getXx() + "," + value.getYy());
            builder.append(",");
        });

        String data = builder.substring(0, builder.length() - 1);

        outputCollector.collect(
                new Text(facilityName),
                new Text(data.toString())
        );
    }
}