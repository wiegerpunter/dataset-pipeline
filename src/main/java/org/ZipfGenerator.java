package org;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;

import java.util.Random;

public class ZipfGenerator {
    public static long[][] zipfData(int numberOfRecords,int numberOfAttributes, int maxValue, double alpha) {
        long[][] data = new long[numberOfRecords][numberOfAttributes];
        Well19937c random = new Well19937c(0);
        ZipfDistribution zipf = new ZipfDistribution(random, maxValue, alpha);
        for (int i=0;i<numberOfRecords;i++) {
            for (int j=0;j<numberOfAttributes;j++) {
                data[i][j] = zipf.sample();
            }
        }
        return data;
    }

    public static ZipfDistribution getZipfDistribution(int maxValue, double alpha, int seed) {
        Well19937c random = new Well19937c(seed);
        return new ZipfDistribution(random, maxValue, alpha);
    }

    public static long[] zipfDataRecord(ZipfDistribution zipf, int numberOfAttributes) {
        long[] record = new long[numberOfAttributes];
        for (int i=0;i<numberOfAttributes;i++) {
            record[i] = zipf.sample();
        }
        return record;
    }

    public static long[][] zipfDataSparse(int numberOfRecords,int numberOfAttributes, int maxValue, double alpha) {
        long[][] data = new long[numberOfRecords][numberOfAttributes];
        Well19937c random = new Well19937c(0);
        ZipfDistribution zipf = new ZipfDistribution(random, maxValue, alpha);
        // sometimes, value is set to -3 bc they're nulls
        Random randomSparse = new Random(1);
        for (int i=0;i<numberOfRecords;i++) {
            for (int j=0;j<numberOfAttributes;j++) {
                if (randomSparse.nextInt() % 5 == 0) {
                    data[i][j] = -3;
                }
                else {
                    data[i][j] = zipf.sample();
                }
            }
        }
        return data;
    }
}
