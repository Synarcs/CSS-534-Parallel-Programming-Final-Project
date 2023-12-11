package com.css534.parallel;

import java.io.Serializable;

public class Vector2f implements Serializable {
    private double xx;
    private double yy;

    public Vector2f(double xx, double yy) {
        this.xx = xx;
        this.yy = yy;
    }

    public double getXx() {
        return xx;
    }

    public double getYy() {
        return yy;
    }

    public void setYy(double yy) {
        this.yy = yy;
    }

    public void setXx(double xx) {
        this.xx = xx;
    }

}