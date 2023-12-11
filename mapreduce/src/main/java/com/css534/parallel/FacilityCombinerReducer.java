package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

import static com.css534.parallel.DelimeterRegexConsts.FACILITY_COUNT;
import static com.css534.parallel.DelimeterRegexConsts.FAVOURABLE_POSITION;

/**
 *      Second Reducer class to apply the min-max algorithm to find the skyline objects
 */
public class FacilityCombinerReducer extends MapReduceBase implements Reducer<GlobalOrderSkylineKey, Text, Text, Text> {
    private JobConf conf;
    private Log log = LogFactory.getLog(FacilityCombinerReducer.class);

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.conf = job;
    }


    @Override
    public void reduce(GlobalOrderSkylineKey compositeIndex,
                       Iterator<Text> projectionValues, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException, IllegalArgumentException {

        /*
            expected will receive values (facilityName, distance)
            this provides unique shuffle sort based on each grid cell in the map can be optimized using a local combiner
        */
        int colNumber = compositeIndex.getColNumber();
        int rowNumber = compositeIndex.getRowNumber();

        // List to hold the segregation of the facilities as favourable and unfaborable
        List<Double> processGridData[] = new ArrayList[FACILITY_COUNT];

        for (int i=0; i < processGridData.length; i++) processGridData[i] = new ArrayList<>();

        StringBuilder builder = new StringBuilder();

        //  (1,3.0)(1,3.0)(0,1.0) (sample input received to the reducer)
        /*
         *   Filter out the result if 1 its favourable else its unfavorable type facility.
         */
        while (projectionValues.hasNext()){
            String[] currentObject = projectionValues.next().toString().trim().split(",");
            if (currentObject.length != 2){
                log.error("Incorrect facilty Object Types Projected");
                throw new IllegalArgumentException();
            }
            if (Integer.parseInt(currentObject[0]) == FAVOURABLE_POSITION)
                processGridData[0].add(Double.valueOf(currentObject[1]));
            else
                processGridData[1].add(Double.valueOf(currentObject[1]));
        }


        /**
         *  Implementation of Min-Max distance algorithm starts
         */
        double globalMaxIndexFav = Double.MIN_VALUE;
        double globalMaxIndexUnFav = Double.MIN_VALUE;

        double globalMinimaIndexFav = Double.MAX_VALUE;
        double globalMinimaIndexUnFav = Double.MAX_VALUE;


        for (Double fd: processGridData[0]){
            globalMinimaIndexFav = Double.min(globalMinimaIndexFav, fd);
            globalMaxIndexFav = Double.max(globalMaxIndexFav, fd);
        }

        for (Double ud: processGridData[1]){
            globalMinimaIndexUnFav = Double.min(globalMinimaIndexUnFav, ud);
            globalMaxIndexUnFav = Double.max(globalMaxIndexUnFav, ud);
        }

        log.info("The size of the reduced columns for all fav grids is " + processGridData[0].size());
        log.info("The size of the reduced columns for all unFav grids is " + processGridData[1].size());

        // // the size should match total number of facilities including fav and unfavourable
        /*
            Considering computational projection in the form of circles to find locus of all points
         */
        if (globalMinimaIndexFav != Double.MAX_VALUE && globalMinimaIndexUnFav != Double.MAX_VALUE){
            if (globalMinimaIndexUnFav < globalMinimaIndexFav || // locus of points for unfavourable are much closer than fav
                    globalMinimaIndexFav == globalMinimaIndexUnFav ||   // both of the locus are equi distant
                        globalMaxIndexFav > globalMinimaIndexUnFav ||   // max locus for all points favourable is greater than the unfav min it means unfav is much closer (the inner circle for unfav)
                        globalMinimaIndexFav > globalMaxIndexUnFav){  // an implicit case mindistance for fav facility is greater than max for unfav
                log.info("The minimum scale object with unFav facility more closer");
                return;
            }
        }

        outputCollector.collect(
                new Text(String.valueOf(rowNumber)),
                new Text(String.valueOf(colNumber))
        );
    }
}