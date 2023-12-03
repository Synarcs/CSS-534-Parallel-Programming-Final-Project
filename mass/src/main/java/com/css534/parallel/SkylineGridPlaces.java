package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.Place;
import static com.css534.parallel.GridConstants.INIT;

@SuppressWarnings("unused")
public class SkylineGridPlaces extends Place {

    private static int myX, myY;
    private static int sizeX, sizeY;
    private double leftSumAgent = Double.MAX_VALUE, rightSumAgent = Double.MAX_VALUE;

    public double getLeftSumAgent() {
        return leftSumAgent;
    }
    public double getRightSumAgent() {
        return rightSumAgent;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public Object callMethod(int functionId, Object argument) {

        switch (functionId){
            case INIT -> {
                return null;
            }
        }

        return null;
    }

    public SkylineGridPlaces(){}

    public void initFacilityGridIndex(){
        int[] currentPosition = getIndex();
        int x = currentPosition[0]; int y = currentPosition[1]; int z = currentPosition[2];;
    }
}
