package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.css534.parallel.DelimeterRegexConsts.*;

/**
 *  Combines all the skyline objects for each each facility and emits
 *  The union is done for each column and then reducer will reduce to make sure we get global unique skyline objects
 */
public class UnionFacilityMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, GlobalSkylineObjects> {
    private Log log = LogFactory.getLog(UnionFacilityMapper.class);
    @Override
    public void map(LongWritable longWritable, Text skylineObjectProjections, OutputCollector<IntWritable, GlobalSkylineObjects> outputCollector, Reporter reporter) throws IOException, ArrayIndexOutOfBoundsException {
        try {
            String objectInfo = skylineObjectProjections.toString();
            String facilityName = objectInfo.split("\\s+")[0];

            Pattern regex = Pattern.compile(SKYLINE_OBJECTS_LOADER);
            Pattern facilityTypeRegex = Pattern.compile(FACILITY_TYPE);

            Matcher matcher = regex.matcher(objectInfo);
            Matcher facilityType = facilityTypeRegex.matcher(facilityName);

            boolean isUnfavorableFacility = facilityType.find();

            if (matcher.find()) {
                String doubleArrayString = matcher.group(1);

                String[] doubleValues = doubleArrayString.split(",");
                // the values are of the format [x1, x2, x3, x4 ... xn];
                if (doubleValues.length == 0) return;

                List<Double> xProjectiosn = Arrays.stream(doubleValues)
                                .mapToDouble(Double::parseDouble)
                                .boxed().collect(Collectors.toList());

                int columnProjection = Integer.parseInt(objectInfo.split("\\s+")[1]);

//                log.info("Extracted double array: " + Arrays.toString(doubleValues));

                for (int i=0; i < xProjectiosn.size(); i++){
                    outputCollector.collect(
                            new IntWritable(columnProjection),
                            new GlobalSkylineObjects(
                                    isUnfavorableFacility ? FAVOURABLE_POSITION : UNFAVOURABLE_POSITION,
                                    xProjectiosn.get(i)
                            )
                    );
                }
            } else {
                log.info("No match found.");
                return;
            }
        }catch (ArrayIndexOutOfBoundsException exception){
            return;
        }
    }
}