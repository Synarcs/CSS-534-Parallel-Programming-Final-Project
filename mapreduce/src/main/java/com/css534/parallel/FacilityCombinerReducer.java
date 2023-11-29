package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.css534.parallel.DelimeterRegexConsts.FACILITY_TYPE;
import static com.css534.parallel.DelimeterRegexConsts.UNFAVOURABLE_POSITION;

public class FacilityCombinerReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {
    private JobConf conf;
    private Log log = LogFactory.getLog(FacilityCombinerReducer.class);

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.conf = job;
    }


    @Override
    public void reduce(IntWritable facilityNameColumn, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        int colNumber = facilityNameColumn.get();
        Set<Double> favourable = new HashSet<>();
        Set<Double> unfavourable = new HashSet<>();
        // later implement hadoop batch Processing

        Map<Double, Integer> favColProjectionMap = new HashMap<>();
        Map<Double, Integer> unFavCalProjectionMap = new HashMap<>();


        // for faster performance consideration later batch segmentation of data can be used.
        while (iterator.hasNext()) {
            String[] type = iterator.next().toString().split("\\s+");
            if (type.length != 2)
                return;
            String facilityType = type[0];
            double xRowProjection = Double.valueOf(type[1]);
            if (facilityType.equals(UNFAVOURABLE_POSITION))
                unFavCalProjectionMap.put(xRowProjection, unFavCalProjectionMap.getOrDefault(xRowProjection, 0) + 1);
            else
                favColProjectionMap.put(xRowProjection, favColProjectionMap.getOrDefault(xRowProjection, 0) + 1);
        }

        for (Double xCordProjection: favColProjectionMap.keySet()){
            if (favColProjectionMap.get(xCordProjection) == Integer.parseInt(this.conf.get("favourableFacilitiesCount"))){
                favourable.add(xCordProjection);
            }
        }
        for (Double xCordProjection: unFavCalProjectionMap.keySet()){
            if (unFavCalProjectionMap.get(xCordProjection) == Integer.parseInt(this.conf.get("unFavourableFacilitiesCount"))) {
                unfavourable.add(xCordProjection);
            }
        }

        favourable.removeAll(unfavourable);

        StringBuilder builder = new StringBuilder();


        favourable.stream().forEach((value) -> {
            builder.append(value);
            builder.append(",");
        });

        String ans = builder.substring(0, builder.length() - 1);

        outputCollector.collect(
                new Text(String.valueOf(colNumber)),
                new Text(ans)
        );
    }
}