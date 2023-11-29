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

public class FacilityCombinerReducer extends MapReduceBase implements Reducer<IntWritable, GlobalSkylineObjects, Text, Text> {
    private JobConf conf;
    private Log log = LogFactory.getLog(FacilityCombinerReducer.class);

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.conf = job;
    }


    @Override
    @SuppressWarnings("unused")
    public void reduce(IntWritable facilityNameColumn, Iterator<GlobalSkylineObjects> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

         int colNumber = facilityNameColumn.get();
         Set<Double> favourable = new HashSet<>();
         Set<Double> unfavourable = new HashSet<>();
         // later implement hadoop batch Processing

         Map<Double, Integer> favColProjectionMap = new HashMap<>();
         Map<Double, Integer> unFavCalProjectionMap = new HashMap<>();

         Pattern facilityTypeRegex = Pattern.compile(FACILITY_TYPE);

         // for faster performance consideration later batch segmentation of data can be used.
         while (iterator.hasNext()) {
             String facilityType = iterator.next().getFacilityType();
             double xRowProjection = iterator.next().getxProjections();
             Matcher facilityTypeMatch = facilityTypeRegex.matcher(facilityType);
             if (facilityTypeMatch.find())
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
            builder.append(favourable + "," + colNumber);
            builder.append(",");
         });

         outputCollector.collect(
                new Text(String.valueOf(colNumber)),
                new Text(builder.toString())
         );
    }
}