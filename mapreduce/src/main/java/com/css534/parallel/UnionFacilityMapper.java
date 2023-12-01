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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.css534.parallel.DelimeterRegexConsts.*;

/**
 *  Combines all the skyline objects for each each facility and emits
 *  The union is done for each column and then reducer will reduce to make sure we get global unique skyline objects
 */
@SuppressWarnings("unused")
public class UnionFacilityMapper extends MapReduceBase implements Mapper<LongWritable, Text, GlobalOrderSkylineKey, GlobalSkylineObjects> {
    private Log log = LogFactory.getLog(UnionFacilityMapper.class);
    private GlobalOrderSkylinePartioner writer = new GlobalOrderSkylinePartioner();
    @Override
    public void map(LongWritable longWritable, Text skylineObjectProjections, OutputCollector<GlobalOrderSkylineKey, GlobalSkylineObjects> outputCollector, Reporter reporter) throws IOException, ArrayIndexOutOfBoundsException {
        try {
            String objectInfo = skylineObjectProjections.toString();
            String[] dataSplit =  objectInfo.split("\\s+");

            String facilityName = dataSplit[0];

            Pattern regex = Pattern.compile(SKYLINE_OBJECTS_LOADER);
            Matcher matcher = regex.matcher(objectInfo);

            boolean isUnfavorableFacility = facilityName.trim().indexOf("-") != -1 ? true : false;

            if (matcher.find()) {
                String doubleArrayString = matcher.group(1).trim();

//                String[] doubleValues = doubleArrayString.split(",");
                // the values are of the format [x1, x2, x3, x4 ... xn];
                if (doubleArrayString.length() == 0){
                    log.error("Error empty input for the string was found");
                    return;
                }

                int columnProjection = Integer.parseInt(dataSplit[1]);
                String[] distanceXProjections = doubleArrayString.split("\\s+");

                if (distanceXProjections.length == 0) {
                    log.error("No Required X Row Projections found");
                    return;
                }

                int rowIndex = 1;
                for (int i=2; i < distanceXProjections.length; i++){
                    outputCollector.collect(
                            new GlobalOrderSkylineKey(rowIndex, columnProjection),
                            new GlobalSkylineObjects(
                                    facilityName, Double.valueOf(distanceXProjections[i])
                            )
                    );
                    rowIndex++;
                }
            } else {
                log.error("No match found.");
                return;
            }
        }catch (ArrayIndexOutOfBoundsException exception){
            return;
        }
    }
}