package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.Agent;

import java.io.Serializable;

import static com.css534.parallel.GridConstants.MOVE_TOURIST;

@SuppressWarnings("unused")
public class SkylineAgent extends Agent implements Serializable {

    public static int MOVE_TOURIST = 1;

    public void handleAgentAction(){
//        getPlace().callMethod()
    }

    @Override
    public Object callMethod(int functionId, Object argument) {
        switch (functionId){
            case 1 -> {
                return null;
            }
        }
        return null;
    }

    @Override
    public int map(int initPopulation, int[] size, int[] index, int offset) {
        return super.map(initPopulation, size, index, offset);
    }

}
