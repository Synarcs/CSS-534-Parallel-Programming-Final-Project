package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Place;

import static com.css534.parallel.GridConstants.INIT;

@SuppressWarnings("unused")
public class SkylineGridPlaces extends Place {

    private static int myX, myY;
    private static int sizeX, sizeY;
    public double leftSumAgent = Double.MAX_VALUE, rightSumAgent = Double.MAX_VALUE;
    public double currentMinValue = 0;

    public double facilityPresentInPlace = 0;

    public double getLeftSumAgent() { return leftSumAgent; }
    public double getRightSumAgent() { return rightSumAgent;}


    public void setCurrentMinValue(){
        if (leftSumAgent == Double.MAX_VALUE && rightSumAgent == Double.MAX_VALUE){
            MASS.getLogger().error("Error please synchronize the agent first befroe call all");
        }

    }
    public void initFacilityObjectAtPlace(){
        // x y z
        int[] currentPosition = getIndex();
        int[] size = getSize();
        int xx= currentPosition[0]; int yy = currentPosition[1]; int zz = currentPosition[2];
        int xSize = size[0]; int ysize = size[1]; int zSize = size[2];
        if (yy >= (int) ysize / 2) facilityPresentInPlace = 1;
    }
    @Override
    public Object callMethod(int functionId, Object argument) {
        switch (functionId){
            case INIT -> initFacilityObjectAtPlace();
        }

        return null;
    }

    public SkylineGridPlaces(){}



}
