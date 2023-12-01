package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

import static com.css534.parallel.DelimeterRegexConsts.FACILITY_COUNT;
import static com.css534.parallel.DelimeterRegexConsts.FAVOURABLE_POSITION;


public class FacilityCombinerReducer extends MapReduceBase implements Reducer<GlobalOrderSkylineKey, GlobalSkylineObjects, Text, Text> {
    private JobConf conf;
    private Log log = LogFactory.getLog(FacilityCombinerReducer.class);

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.conf = job;
    }


    @Override
    public void reduce(GlobalOrderSkylineKey compositeIndex,
                            Iterator<GlobalSkylineObjects> projectionValues, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        /*
            expected will receive values (facilityName, distance)
            this provides unique shuffle sort based on each grid cell in the map can be optimized using a local combiner
        */
        int colNumber = compositeIndex.getColNumber();
        int rowNumber = compositeIndex.getRowNumber();

        List<GlobalSkylineObjects> processGridData[] = new ArrayList[FACILITY_COUNT];

        while (projectionValues.hasNext()){
            GlobalSkylineObjects currentObject = projectionValues.next();
            if (currentObject.getFacilityType().equals(FAVOURABLE_POSITION)){
                processGridData[0].add(currentObject);
            }else
                processGridData[1].add(currentObject);
        }

        double globalMinimaIndexFav = Double.MAX_VALUE;
        double globalMinimaIndexUnFav = Double.MAX_VALUE;

        for (int i=0; i < processGridData[0].size(); i++)
            globalMinimaIndexFav = Double.min(globalMinimaIndexFav, processGridData[0].get(i).getxProjectionsValue());

        for (int i=0; i < processGridData[1].size(); i++)
            globalMinimaIndexUnFav = Double.min(globalMinimaIndexUnFav, processGridData[1].get(i).getxProjectionsValue());

        log.info("The size of the reduced columns for all fav grids is " + processGridData[0].size());
        log.info("The size of the reduced columns for all unFav grids is " + processGridData[1].size());

        // the size should match total number of facilities including fav and unfavourable
        if (globalMinimaIndexUnFav < globalMinimaIndexFav || globalMinimaIndexFav == globalMinimaIndexUnFav){
            log.info("The minimum scale object with unFav facility more closer");
            return;
        }

        StringBuilder builder = new StringBuilder();
        String ans = builder.substring(0, builder.length() - 1);

        outputCollector.collect(
                new Text(String.valueOf(rowNumber)),
                new Text(String.valueOf(colNumber))
        );
    }
}