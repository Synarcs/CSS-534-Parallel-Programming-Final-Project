package com.css534.parallel;

import edu.uw.bothell.css.dsl.MASS.Agent;

public class SkylineGridAgent extends Agent {

    public SkylineGridAgent(Object argument){ super();}
    public SkylineGridAgent() { super();}
    public Object solve(Object argument){
        int agentId = getAgentId();
        int placeIndex = getPlace().getIndex()[0];
        return getPlace()
                .callMethod(3, argument);
    }

    @Override
    public Object callMethod(int functionId, Object argument) {
        switch (functionId){
            case 0: solve(argument);
        }
        return null;
    }
}
