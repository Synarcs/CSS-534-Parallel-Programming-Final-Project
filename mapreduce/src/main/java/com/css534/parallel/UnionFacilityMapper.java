package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.css534.parallel.DelimeterRegexConsts.SKYLINE_OBJECTS_LOADER;

/**
 *  Combines all the skyline objects for each each facility and emits
 *  The union is done for each column and then reducer will reduce to make sure we get global unique skyline objects
 */
public class UnionFacilityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Log log = LogFactory.getLog(UnionFacilityMapper.class);
    @Override
    public void map(LongWritable longWritable, Text skylineObjectProjections, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException, ArrayIndexOutOfBoundsException {
        try {
            String objectInfo = skylineObjectProjections.toString();
            String facilityName = objectInfo.split("\\s+")[0];
            Pattern regex = Pattern.compile("\\[([^\\]]+)\\]");
            Matcher matcher = regex.matcher(objectInfo);


            if (matcher.find()) {
                String doubleArrayString = matcher.group(1);

                String[] doubleValues = doubleArrayString.split(",");

                System.out.println("Extracted double array: " + Arrays.toString(doubleValues));

                for (int i=0; i < doubleValues.length; i+=2){
                    outputCollector.collect(
                            new Text(facilityName),
                            new Text(
                                    Arrays.toString(doubleValues)
                            )
                    );
                }
            } else {
                System.out.println("No match found.");
            }
        }catch (ArrayIndexOutOfBoundsException exception){
            return;
        }
    }
}
