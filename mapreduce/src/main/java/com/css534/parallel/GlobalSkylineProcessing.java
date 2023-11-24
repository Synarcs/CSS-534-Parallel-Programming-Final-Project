package com.css534.parallel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

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

        }
    }

}
