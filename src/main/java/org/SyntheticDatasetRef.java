package org;

import org.apache.commons.math3.distribution.ZipfDistribution;

import java.io.*;
import java.util.*;

public class SyntheticDatasetRef {
    private final Config config;
    private String datasetFileName;
    private int datasetSize;
    public SyntheticDatasetRef(Config config) {
        this.config = config;
        config.domain = 10000;
    }

    public void synthDevDataGenerator(int numZipfAttributes, double sizeFactor,
                                  double zipfAlpha) {
        setupDataset(numZipfAttributes, sizeFactor, zipfAlpha);
        generateSynthDataset(zipfAlpha);
    }

    private void setupDataset(int numAttrs, double sizeFactor, double zipfAlpha) {
        int domain = config.domain;
        int numZipfianAttrs = config.numZipfAttributes;
        int numUniformAttrs = config.numUniformAttributes;
        datasetSize = (int) Math.pow(2, sizeFactor);
        datasetFileName = setDatasetName(numAttrs, domain, sizeFactor, datasetSize, numZipfianAttrs, zipfAlpha, numUniformAttrs);
    }

    private String setDatasetName(int numAttrs, int domain, double sizeFactor, int datasetSize,
                                  int numZipfianAttrs, double zipfAlpha, int numUniformAttrsc) {
        return config.readFolder + "/input/data/synthFromDisk/" + numAttrs + "/zipfAlpha_" + zipfAlpha + "/" + sizeFactor + "/cleanFile.csv";
    }

    private long[] createRecord (int index, ZipfDistribution zipf, Random unifRandom, int numAttrs, int numZipfianAttrs, int numUniformAttrs, int domain, double zipfAlpha) {
        long[] record = new long[numAttrs + 1]; // +1 for id
        record[0] = index; // id
        if (numZipfianAttrs > 0) {
            long[] zipfData = ZipfGenerator.zipfDataRecord(zipf, numZipfianAttrs);
            System.arraycopy(zipfData, 0, record, 1, numZipfianAttrs);
        }
        if (numUniformAttrs > 0) {
            long[] unifData = new long[numUniformAttrs];
            for (int j = 0; j < numUniformAttrs; j++) {
                unifData[j] = unifRandom.nextInt(domain);
            }
            System.arraycopy(unifData, 0, record, 1 + numZipfianAttrs, numUniformAttrs);
        }

        return record;
    }

    private void generateSynthDataset(double zipfAlpha) {
        int numAttrs = config.numStoredAttributes;
        int domain = config.domain;
        int numZipfianAttrs = config.numZipfAttributes;
        int numUniformAttrs = config.numUniformAttributes;
        if (numAttrs != numZipfianAttrs + numUniformAttrs) {
            throw new IllegalArgumentException("Number of attributes must be sum of Zipfian and Uniform attributes (" +
                    numZipfianAttrs + " + " + numUniformAttrs + ") but got " + numAttrs);
        }
        try (BufferedWriter writer = getBufferedWriter(datasetFileName)) {
            writeHeader(writer, numAttrs);
            ZipfDistribution zipfResidu = ZipfGenerator.getZipfDistribution(domain, zipfAlpha, 0);
            Random unifRandom = new Random(0);

            for (int i = 0; i < datasetSize; i++) {
                // Create a record
                long[] record = createRecord(i, zipfResidu, unifRandom, numAttrs, numZipfianAttrs, numUniformAttrs, domain, zipfAlpha);
                writeRecord(writer, record, 1);
            }

        } catch (IOException except)
        {
            System.err.println("Error opening file for writing: " + datasetFileName);
            except.printStackTrace();
        }
    }

    private void writeRecord(BufferedWriter writer, long[] record, int sign) {
        String[] recordStr = Arrays.stream(record)
                .mapToObj(String::valueOf)
                .toArray(String[]::new);
        recordStr = Arrays.copyOf(recordStr, recordStr.length + 1);
        recordStr[recordStr.length - 1] = String.valueOf(sign); // Append sign at the end
        try {
            writeLine(writer, recordStr);
        } catch (IOException e) {
            System.err.println("Error writing record to file: " + datasetFileName);
            e.printStackTrace();
        }
    }

    private void writeHeader(BufferedWriter writer, int numAttrs) {
        String[] header = new String[numAttrs + 2];
        header[0] = "id";
        for (int i = 1; i <= numAttrs; i++) {
            header[i] = "attr" + i;
        }
        header[header.length - 1] = "sign"; // Append sign at the end
        try {
            writeLine(writer, header);
        } catch (IOException e) {
            System.err.println("Error writing header to file: " + datasetFileName);
            e.printStackTrace();
        }
    }

    private BufferedWriter getBufferedWriter(String filename) throws IOException {
        // check if dir exists, if not create it
        File file = new File(filename);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        return new BufferedWriter(new FileWriter(filename));
    }

    private void writeLine(BufferedWriter writer, String... values) throws IOException {
        writer.write(String.join(",", values));
        writer.newLine();
    }

}
