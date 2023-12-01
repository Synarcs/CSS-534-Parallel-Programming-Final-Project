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

import javax.management.RuntimeErrorException;
import javax.sound.midi.SysexMessage;

import static com.css534.parallel.DelimeterRegexConsts.*;

/**
 *  Combines all the skyline objects for each each facility and emits
 *  The union is done for each column and then reducer will reduce to make sure we get global unique skyline objects
 */
@SuppressWarnings("unused")
public class UnionFacilityMapper extends MapReduceBase implements Mapper<LongWritable, Text, GlobalOrderSkylineKey, Text> {
    private Log log = LogFactory.getLog(UnionFacilityMapper.class);
    private GlobalOrderSkylinePartioner writer = new GlobalOrderSkylinePartioner();
    @Override
    public void map(LongWritable longWritable, Text skylineObjectProjections, OutputCollector<GlobalOrderSkylineKey, Text> outputCollector, Reporter reporter) throws IOException,
            IllegalArgumentException,ArrayIndexOutOfBoundsException {
        String objectInfo = skylineObjectProjections.toString();
        int indexPipe = objectInfo.indexOf("||");
        if (indexPipe == -1) {
            log.error("Incorrect Projection format");
            throw new IllegalArgumentException();
        }

        String projectionValues = objectInfo.substring(0, indexPipe);
        String[] dataSplit =  projectionValues.split("\\s+");

        if (dataSplit.length == 0) {
            log.error("No Required X Row Projections found");
            throw new IllegalArgumentException();
        }

        String facilityName = dataSplit[0];
        Integer columnProjection = Integer.parseInt(dataSplit[1]);

        boolean isUnfavorableFacility = facilityName.trim().indexOf("-") != -1 ? true : false;

        int rowIndex = 1;
        for (int i=2; i < dataSplit.length; i++){
            GlobalOrderSkylineKey key = new GlobalOrderSkylineKey(rowIndex, columnProjection);

            // System.out.println(key.getColNumber() + " " + key.getRowNumber() + " " + value.getFacilityType() + " " + value.getxProjectionsValue());

            // throw new RuntimeException();
            if (isUnfavorableFacility){
                outputCollector.collect(
                        new GlobalOrderSkylineKey(rowIndex, columnProjection),
                        new Text(
                                String.valueOf(UNFAVOURABLE_POSITION + "," + Double.valueOf(dataSplit[i]))
                        )
                );
            }else{
                outputCollector.collect(
                        new GlobalOrderSkylineKey(rowIndex, columnProjection),
                        new Text(
                                String.valueOf(FAVOURABLE_POSITION + "," + Double.valueOf(dataSplit[i]))
                        )
                );
            }
            rowIndex++;
        }
    }
}