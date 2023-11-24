package com.css534.parallel;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Vector2f implements Serializable, Writable {

    private double xx;
    private double yy;

    public Vector2f() {}
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



    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Vector2f point = (Vector2f) obj;
        return Double.compare(point.xx, xx) == 0 && Double.compare(point.yy, yy) == 0;
    }


    @Override
    public int hashCode() {
        int result = Double.hashCode(xx);
        result = 31 * result + Double.hashCode(yy);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(xx);
        dataOutput.writeDouble(yy);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        xx = dataInput.readDouble();
        yy = dataInput.readDouble();
    }
}
