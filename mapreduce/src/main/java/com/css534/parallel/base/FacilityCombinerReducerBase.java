package com.css534.parallel.base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import com.css534.parallel.FacilityCombinerReducer;
import com.css534.parallel.GlobalOrderSkylineKey;

import java.io.IOException;
import java.util.*;

import static com.css534.parallel.DelimeterRegexConsts.FACILITY_COUNT;
import static com.css534.parallel.DelimeterRegexConsts.FAVOURABLE_POSITION;
import static com.css534.parallel.DelimeterRegexConsts.UNFAVOURABLE_POSITION;


/**
 *  This reducer class is only menat for debugging the calculation for the global skyline objects
 */
@SuppressWarnings("unused")
public class FacilityCombinerReducerBase extends MapReduceBase implements Reducer<GlobalOrderSkylineKey, Text, Text, Text> {
    private JobConf conf;
    private Log log = LogFactory.getLog(FacilityCombinerReducer.class);

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.conf = job;
    }


    @Override
    public void reduce(GlobalOrderSkylineKey compositeIndex,
                       Iterator<Text> projectionValues, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        /*
            expected will receive values (facilityName, distance)
            this provides unique shuffle sort based on each grid cell in the map can be optimized using a local combiner
        */
        int colNumber = compositeIndex.getColNumber();
        int rowNumber = compositeIndex.getRowNumber();

        List<Double> processGridData[] = new ArrayList[FACILITY_COUNT];

        for (int i=0; i < processGridData.length; i++) processGridData[i] = new ArrayList<>();

        StringBuilder builder = new StringBuilder();

        while (projectionValues.hasNext()){
            String  currentObject = projectionValues.next().toString().trim();
            builder.append("(");
            builder.append(currentObject);
            builder.append(")");
        }

        log.info("The size of the reduced columns for all fav grids is " + processGridData[0].size());
        log.info("The size of the reduced columns for all unFav grids is " + processGridData[1].size());

        outputCollector.collect(
                new Text(String.valueOf(colNumber + " " + rowNumber)),
                new Text(builder.toString())
        );
    }
}