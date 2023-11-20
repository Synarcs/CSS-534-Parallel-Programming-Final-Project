package com.css534.parallel;


public class Vector2f {

    private double xx;
    private double yy;

    Vector2f(double xx, double yy){this.xx = xx; this.yy = yy;}

    public double getXx() {
        return xx;
    }

    public double getYy() {
        return yy;
    }

    public void setXx(double xx) {
        this.xx = xx;
    }

    public void setYy(double yy) {
        this.yy = yy;
    }
}
