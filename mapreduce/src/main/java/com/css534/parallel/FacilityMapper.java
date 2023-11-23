package com.css534.parallel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import static com.css534.parallel.DelimeterConsts.SKYLINE_SEPARATOR;
import static com.css534.parallel.DelimeterConsts.GRID_INDEX;

import java.io.IOException;

public class FacilityMapper extends MapReduceBase implements Mapper<LongWritable, Text, MapKeys, MapValue> {


    @Override
    public void map(LongWritable longWritable, Text featuredColimns, OutputCollector<MapKeys, MapValue> outputCollector, Reporter reporter) throws IOException {
        String[] facilityInfo = featuredColimns.toString().split(SKYLINE_SEPARATOR);

        String objects = facilityInfo[facilityInfo.length - 1];
    }
}
