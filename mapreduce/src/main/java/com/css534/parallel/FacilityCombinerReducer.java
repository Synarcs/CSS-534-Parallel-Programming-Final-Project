package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class FacilityCombinerReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private JobConf conf;
    private Log log = LogFactory.getLog(FacilityCombinerReducer.class);

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.conf = job;
    }

    @Override
    public void reduce(Text facilityName, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        Set<Vector2f> uniqueLocalSkylineObjects = new LinkedHashSet<>();
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