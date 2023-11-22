package com.css534.parallel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class SkylineObjects implements Serializable {
    private List<Double> distances;
    private List<Vector2f> skylineObjects;

    SkylineObjects(){
        this(new ArrayList<>(), new ArrayList<>());
    }
    SkylineObjects(List<Double> distances, List<Vector2f> skylineObjects){
        this.distances = distances;
        this.skylineObjects = skylineObjects;
    }

    public List<Double> getDistances() {
        return distances;
    }

    public List<Vector2f> getSkylineObjects() {
        return skylineObjects;
    }
}
