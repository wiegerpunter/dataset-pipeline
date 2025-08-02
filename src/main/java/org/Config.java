package org;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Config {
    public String readFolder;
    public List<Long> ramVals;
    public int numPredicates;


    public int numZipfAttributes;
    public int numUniformAttributes;
    public int numQueries;
    public int numStoredAttributes;
    public String currentDate;
    public int domain;
    public int sizeFactor;
    public double zipfAlpha;
    public double[] perc;


    public String getInputFolder() {
        return readFolder + "/input/";
    }
    public String getOutputFolder() {
        return readFolder + "/output/";
    }

    public void ramMBToBits() {
        ramVals.replaceAll(aLong -> aLong * 1000000 * 8);
    }

    public void setCurrentDate() {
        currentDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }
}