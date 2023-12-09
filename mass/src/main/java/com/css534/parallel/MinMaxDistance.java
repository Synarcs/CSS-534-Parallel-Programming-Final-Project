package com.css534.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@SuppressWarnings("unused")
public class MinMaxDistance {

    private final int FACILITY_CATEGROY = 2;

    public boolean processMinMaxDistanceAlgorithm(double[] distances, int favCount, int unFavCount,
                                                  int row, int col) {

        int totalFacilities = favCount + unFavCount;
        List<Double> processGridData[] = new ArrayList[FACILITY_CATEGROY];


        for (int i=0; i < processGridData.length; i++) processGridData[i] = new ArrayList<>();

        // consider the global minmax with distance until fav
        for (int i=0 ; i < favCount; i++)
            processGridData[0].add(distances[i]);

        // after the fav index every peice in unfav
        for (int i=favCount; i < totalFacilities; i++)
            processGridData[1].add(distances[i]);

        // System.out.println("teh array is " + Arrays.toString(processGridData));
        double globalMaxIndexFav = Double.MIN_VALUE;
        double globalMaxIndexUnFav = Double.MIN_VALUE;

        double globalMinimaIndexFav = Double.MAX_VALUE;
        double globalMinimaIndexUnFav = Double.MAX_VALUE;

        for (Double fd: processGridData[0]){
            globalMinimaIndexFav = Double.min(globalMinimaIndexFav, fd);
            globalMaxIndexFav = Double.min(globalMaxIndexFav, fd);
        }

        for (Double ud: processGridData[1]){
            globalMinimaIndexUnFav = Double.min(globalMinimaIndexUnFav, ud);
            globalMaxIndexUnFav = Double.max(globalMaxIndexUnFav, ud);
        }

        // // the size should match total number of facilities including fav and unfavourable
        if (globalMinimaIndexFav != Double.MAX_VALUE && globalMinimaIndexUnFav != Double.MAX_VALUE){
            if (globalMinimaIndexUnFav < globalMinimaIndexFav ||
                    globalMinimaIndexFav == globalMinimaIndexUnFav ||
                    globalMaxIndexFav > globalMinimaIndexUnFav ||
                    globalMinimaIndexFav > globalMaxIndexUnFav){
                return false;
            }else {
                return true;
            }
        }

        return false;
    }
}
