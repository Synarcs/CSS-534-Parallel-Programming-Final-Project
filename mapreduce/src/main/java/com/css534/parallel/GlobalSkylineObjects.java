package com.css534.parallel;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GlobalSkylineObjects implements Writable {

    private String facilityType;
    private double xProjectionsValue;

    public double getxProjectionsValue() {
        return xProjectionsValue;
    }

    public String getFacilityType() {
        return facilityType;
    }

    public GlobalSkylineObjects(){}

    public GlobalSkylineObjects(String facilityType, double xProjectionsValue){
        this.facilityType = facilityType;
        this.xProjectionsValue = xProjectionsValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(xProjectionsValue);
        dataOutput.writeUTF(facilityType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        facilityType = dataInput.readUTF();
        xProjectionsValue = dataInput.readDouble();
    }
}
