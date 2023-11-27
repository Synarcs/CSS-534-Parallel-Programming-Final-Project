package com.css534.parallel;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class GlobalSkylineObjects implements Writable, Serializable {

    private String facilityType;
    private double xProjections;

    public double getxProjections() {
        return xProjections;
    }

    public String getFacilityType() {
        return facilityType;
    }

    public GlobalSkylineObjects(String facilityType, double xProjections){
        this.facilityType = facilityType;
        this.xProjections = xProjections;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(xProjections);
        dataOutput.writeUTF(facilityType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        facilityType = dataInput.readUTF();
        xProjections = dataInput.readDouble();
    }
}
