package com.css534.parallel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.css534.parallel.DelimeterRegexConsts.GRID_INDEX;

public class GlobalSkylineProcessing implements Serializable {

    public static class GlobalSkylineProcessingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(
                    new Text(GRID_INDEX),
                    new Text(text)
            );
        }
    }

    public static class GlobalSkylineProcessingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            Set<Vector2f> globalFavourable = new LinkedHashSet<>();
            Set<Vector2f> globalUnFavourable = new LinkedHashSet<>();


            while (iterator.hasNext()){
                String globalPoints = iterator.toString();
                String[] points = globalPoints.split("\\s+");

                String facilityName = points[0];
                double[] coordinates = Arrays.stream(points[1].split(","))
                        .mapToDouble(Double::parseDouble)
                        .toArray();

                if (facilityName.contains("-")){
                    // a unfavourable facility
                    for (int i=0; i < coordinates.length; i+=2){
                        globalUnFavourable.add(
                                new Vector2f(
                                        coordinates[i],
                                        coordinates[i+1]
                                )
                        );
                    }
                }else {
                    for (int i=0; i < coordinates.length; i+=2){
                        globalFavourable.add(
                                new Vector2f(
                                        coordinates[i],
                                        coordinates[i+1]
                                )
                        );
                    }
                }
            }

            globalFavourable.removeAll(globalUnFavourable);
        }
    }

}
