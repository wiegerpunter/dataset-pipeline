package org;

import org.apache.commons.math3.distribution.ZipfDistribution;

import java.io.*;
import java.util.*;

public class SyntheticDataset {
    private final Config config;
    private String datasetFileName;
    private String queryFileName;
    private int datasetSize;
    private int noiseSize;
    private long[][] pointQueries;
    private int[] pointQueryAnswers;
    private int[] pointQueryUnion;

    private int[] pointQueriesNumAttrs;
    private int[] pointQueryBinNumber;
    private int[] pointQueriesNumZipfian;

    public SyntheticDataset(Config config) {
        this.config = config;
        config.domain = 10000;
        int numQueries = config.numQueries * config.numPredicates;
        pointQueries = new long[numQueries][];
    }

    public String getDatasetFileName() {
        return datasetFileName;
    }
    public String getQueryFileName() {
        return queryFileName;
    }

    public void synthDevDataGenerator(double perc, double sizeFactor,
                                  double zipfAlpha) {
        setupDataset(perc, sizeFactor, zipfAlpha);
        generateSynthDataset(zipfAlpha);

    }

    public void synthDevQueryGenerator(double perc, double sizeFactor,
                                  double zipfAlpha) throws IOException {
        setupDataset(perc, sizeFactor, zipfAlpha);
        generateSynthQueries();
    }

    public void synthDevLoader(double perc, double sizeFactor, double zipfAlpha) {
        setupDataset(perc, sizeFactor, zipfAlpha);
        loadQueries(perc);
    }

    private void setupDataset(double perc, double sizeFactor, double zipfAlpha) {
        int numAttrs = 9;
        int domain = config.domain;
        int numZipfianAttrs = config.numZipfAttributes;
        int numUniformAttrs = config.numUniformAttributes;
        datasetSize = (int) Math.pow(2, sizeFactor);
        noiseSize = (int) (datasetSize * perc);
        datasetFileName = setDatasetName(numAttrs, domain, sizeFactor, datasetSize, numZipfianAttrs, zipfAlpha, numUniformAttrs, perc);
    }

    private String setDatasetName(int numAttrs, int domain, double sizeFactor, int datasetSize,
                                  int numZipfianAttrs, double zipfAlpha, int numUniformAttrs,
                                  double perc) {
        return config.readFolder + "/input/synthFromDisk/zipfAlpha_" + zipfAlpha + "/" + sizeFactor + "/" + perc + "/" + "syntheticDataset_" + numAttrs + "_" + domain + "_" + sizeFactor + "_" + datasetSize + "_" +
                numZipfianAttrs + "_" + zipfAlpha + "_" + numUniformAttrs + "_" + perc + ".csv";
    }

//    private String setQueryFileName(String datasetFileName) {
//        if (datasetFileName.endsWith(".csv")) {
//            return datasetFileName.substring(0, datasetFileName.length() - 4) + "_queries.csv";
//        } else {
//            throw new IllegalArgumentException("Dataset filename does not end with .csv");
//        }
//    }

    private String setQueryFileName(String datasetFileName, double perc) {
        File datasetFile = new File(datasetFileName);
        File percFolder = datasetFile.getParentFile();  // e.g., .../5.0/0.3
        File sizeFactorFolder = percFolder.getParentFile();  // e.g., .../5.0

        if (datasetFileName.endsWith(".csv")) {
            String queryFileName = datasetFile.getName().replace(".csv", "_queries.csv");
            // Find positions of the last two underscores
            int lastUnderscore = queryFileName.lastIndexOf("_");
            int secondLastUnderscore = queryFileName.lastIndexOf("_", lastUnderscore - 1);

            if (secondLastUnderscore != -1 && lastUnderscore != -1 && lastUnderscore > secondLastUnderscore) {
                String before = queryFileName.substring(0, secondLastUnderscore + 1);
                String after = queryFileName.substring(lastUnderscore); // includes the underscore and the .csv part

                String modified = before + "0.0" + after;

                System.out.println(modified);
                File queryFile = new File(sizeFactorFolder, "0.0/" + modified);
                return queryFile.getAbsolutePath();
            } else {
                throw new IllegalArgumentException("Dataset filename does not contain the expected underscores for modification: " + datasetFileName);
            }

        } else {
            throw new IllegalArgumentException("Dataset filename does not end with .csv");
        }
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
        int numAttrs = 9;
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
            double zipfNoiseAlpha = zipfAlpha - 0.2; // Slightly lower alpha for noise
            ZipfDistribution zipfNoise = ZipfGenerator.getZipfDistribution(domain, zipfNoiseAlpha, 1);

            Random unifRandom = new Random(0);

            for (int i = 0; i < datasetSize; i++) {
                // Create a record
                long[] record = createRecord(i, zipfResidu, unifRandom, numAttrs, numZipfianAttrs, numUniformAttrs, domain, zipfAlpha);
                writeRecord(writer, record, 1);
            }
            for (int i = 0; i < noiseSize; i++) {
                // Create a noise record
                long[] record = createRecord(i + datasetSize, zipfNoise, unifRandom, numAttrs, numZipfianAttrs, numUniformAttrs, domain, zipfNoiseAlpha);
                writeRecord(writer, record, 1);
                writeRecord(writer, record, -1);
            }
        } catch (IOException except)
        {
            System.err.println("Error opening file for writing: " + datasetFileName);
            except.printStackTrace();
        }
    }



    private void generateSynthQueries() throws IOException {
        int numAttrs = 9;
        int numZipfianAttrs = config.numZipfAttributes;

        Set<Integer> selectedIndices = selectRandomIndices(datasetSize, config.numQueries);
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetFileName))) {
            populatePointQueries(reader, numAttrs, selectedIndices);
        } catch (IOException e) {
            System.err.println("Error opening file for queries: " + datasetFileName);
            throw e;
        }

        pointQueryAnswers = new int[pointQueries.length];
        pointQueryUnion = new int[pointQueries.length];
        pointQueriesNumAttrs = new int[pointQueries.length];
        pointQueryBinNumber = new int[pointQueries.length];
        pointQueriesNumZipfian = new int[pointQueries.length];
        applyPredicates(numAttrs);
        deduplicateQueries(numAttrs);
        computeExactAnswers();
        computeQueryStats(numAttrs, numZipfianAttrs);
    }


    private void applyPredicates(int numAttrs) {
        Random randomQueries = new Random(0);

        for (int p = 0; p < config.numPredicates; p++) {
            for (int i = p * config.numQueries; i < (p + 1) * config.numQueries; i++) {
                int curNumPreds = 0;
                while (curNumPreds < numAttrs - (p + 1)) {
                    int index = randomQueries.nextInt(numAttrs);
                    if (pointQueries[i][index] != -1) {
                        pointQueries[i][index] = -1;
                        curNumPreds++;
                    }
                }
            }
        }
    }
    private void deduplicateQueries(int numAttrs) {
        // Deduplication process
        Set<String> uniqueQueries = new HashSet<>();
        int uniqueCount = 0;

        for (long[] query : pointQueries) {
            String key = buildQueryKey(query, numAttrs);

            if (uniqueQueries.add(key)) {
                pointQueries[uniqueCount] = query;
                uniqueCount++;
            }
        }
        // Resize the pointQueries array to contain only unique entries
        pointQueries = Arrays.copyOf(pointQueries, uniqueCount);
    }
    private String buildQueryKey(long[] query, int numAttrs) {
        StringBuilder keyBuilder = new StringBuilder();
        for (int j = 0; j < numAttrs; j++) {
            if (query[j] != -1) {
                keyBuilder.append(j).append(":").append(query[j]).append(";");
            }
        }
        return keyBuilder.toString();
    }

    private void computeExactAnswers() {
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetFileName))) {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("id")) continue;

                long[] record = readRecord(line, pointQueries[0].length);
                updateAnswers(record);
            }
        } catch (IOException e) {
            System.err.println("Error computing exact answers.");
            e.printStackTrace();
        }

        writeQueriesToFile();
    }

    private void updateAnswers(long[] record) {
        for (int i = 0; i < pointQueries.length; i++) {
            boolean match = true;
            boolean unionMatch = false;

            for (int j = 0; j < pointQueries[i].length; j++) {
                long queryVal = pointQueries[i][j];
                long recordVal = record[j + 1];

                if (queryVal != -1 && queryVal != recordVal) {
                    match = false;
                    break;
                }
                if (queryVal != -1 && queryVal == recordVal) {
                    unionMatch = true;
                }
            }

            if (match) pointQueryAnswers[i]++;
            if (unionMatch) pointQueryUnion[i]++;
        }
    }

    private void writeQueriesToFile() {
        queryFileName = setQueryFileName(datasetFileName, 0.0);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(queryFileName))) {
            writeHeaderQueryFile(writer, pointQueries[0].length);
            for (int i = 0; i < pointQueries.length; i++) {
                StringBuilder line = new StringBuilder();
                line.append(i);

                for (long val : pointQueries[i]) {
                    line.append(",").append(val);
                }
                line.append(",").append(pointQueryAnswers[i])
                        .append(",").append(pointQueryUnion[i]);

                writer.write(line.toString());
                writer.newLine();
            }

        } catch (IOException e) {
            System.err.println("Error writing queries to file.");
            e.printStackTrace();
        }
    }

    private void writeHeaderQueryFile(BufferedWriter writer, int numAttrs) {
        String[] header = new String[numAttrs + 3];
        header[0] = "id";
        for (int i = 1; i <= numAttrs; i++) {
            header[i] = "attr" + i;
        }
        header[header.length - 2] = "answer"; // Append answerat the end
        header[header.length - 1] = "union"; // Append union at the end
        try {
            writeLine(writer, header);
        } catch (IOException e) {
            System.err.println("Error writing header to file: " + datasetFileName);
            e.printStackTrace();
        }
    }

    private void computeQueryStats(int numAttrs, int numZipfianAttrs) {
        for (int i = 0; i < pointQueries.length; i++) {
            for (int j = 0; j < numAttrs; j++) {
                if (pointQueries[i][j] != -1) {
                    pointQueriesNumAttrs[i]++;
                    if (j < numZipfianAttrs) pointQueriesNumZipfian[i]++;
                }
            }
            pointQueryBinNumber[i] = i;
        }
    }

    private void populatePointQueries(BufferedReader reader, int numAttrs, Set<Integer> selectedIndices) throws IOException {
        String line;
        int added = 0;
        while ((line = reader.readLine()) != null && added < pointQueries.length) {
            if (line.startsWith("id")) continue;
            long[] record = readRecord(line, numAttrs);
            int id = (int) record[0];
            if (selectedIndices.contains(id)) {
                for (int p = 0; p < config.numPredicates; p++) {
                    pointQueries[added] = new long[numAttrs];
                    System.arraycopy(record, 1, pointQueries[added], 0, numAttrs);
                    added++;
                }
            }
        }

        // shrink the pointQueries array to the actual number of queries added
        pointQueries = Arrays.copyOf(pointQueries, added);

    }

    private void loadQueries(double perc) {
        ArrayList<long[]> pointQueriesList = new ArrayList<>();
        ArrayList<Integer> pointQueryAnswersList = new ArrayList<>();
        ArrayList<Integer> pointQueryUnionList = new ArrayList<>();
        pointQueryAnswers = new int[pointQueries.length];
        pointQueryUnion = new int[pointQueries.length];
        pointQueriesNumAttrs = new int[pointQueries.length];
        pointQueryBinNumber = new int[pointQueries.length];
        pointQueriesNumZipfian = new int[pointQueries.length];
        queryFileName = setQueryFileName(datasetFileName, perc);
        int numAttrs = config.numStoredAttributes;
        // load queries and pointQueryAnswers from file
        try (BufferedReader reader = new BufferedReader(new FileReader(queryFileName))) {
            String line;
            int queryCount = 0;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("id")) continue; // Skip header line

                String[] parts = line.split(",");
                if (parts.length != numAttrs + 3) { // +2 for answer and union
                    throw new RuntimeException("Record does not match expected number of attributes: " + line);
                }
                long[] query = new long[numAttrs];
                for (int i = 0; i < numAttrs; i++) {
                    query[i] = Long.parseLong(parts[i + 1]); // Skip id
                }
                pointQueriesList.add(query);
                pointQueryAnswersList.add(Integer.parseInt(parts[numAttrs + 1]));
                pointQueryUnionList.add(Integer.parseInt(parts[numAttrs + 2]));
                queryCount++;
            }
        } catch (IOException e) {
            System.err.println("Error reading dataset file: " + datasetFileName);
            e.printStackTrace();
        }

        // Convert ArrayLists to arrays
        pointQueries = new long[pointQueriesList.size()][numAttrs];
        pointQueryAnswers = new int[pointQueries.length];
        pointQueryUnion = new int[pointQueries.length];
        pointQueriesNumAttrs = new int[pointQueries.length];
        pointQueriesNumZipfian = new int[pointQueries.length];
        pointQueryBinNumber = new int[pointQueries.length];
        for (int i = 0; i < pointQueriesList.size(); i++) {
            pointQueries[i] = pointQueriesList.get(i);
            pointQueryAnswers[i] = pointQueryAnswersList.get(i);
            pointQueryUnion[i] = pointQueryUnionList.get(i);
        }

        computeQueryStats(numAttrs, config.numZipfAttributes);
    }


    BufferedReader getDatasetReader(String filename) {
        try {
            return new BufferedReader(new FileReader(filename));
        } catch (IOException e) {
            System.err.println("Error opening file: " + filename);
            e.printStackTrace();
            return null;
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

    private long[] readRecord(String line, int numAttrs) {
        String[] parts = line.split(",");
        if (parts.length != numAttrs + 2) {
            throw new IllegalArgumentException("Record does not match expected number of attributes: " + line);
        }
        long[] record = new long[numAttrs + 2];
        for (int i = 0; i < parts.length; i++) {
            record[i] = Long.parseLong(parts[i]);
        }
        return record;
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

    private BufferedReader getBufferedReader(String filename) throws IOException {
        return new BufferedReader(new FileReader(filename));
    }

    private void writeLine(BufferedWriter writer, String... values) throws IOException {
        writer.write(String.join(",", values));
        writer.newLine();
    }

    private Set<Integer> selectRandomIndices(int datasetSize, int numQueries) {
        Set<Integer> randomIndices = new HashSet<>();
        Random random = new Random(0);
        while (randomIndices.size() < numQueries && randomIndices.size() < datasetSize) {
            int index = random.nextInt(datasetSize);
            randomIndices.add(index);
        }
        if (randomIndices.size() < numQueries) {
            config.numQueries = randomIndices.size(); // Adjust numQueries if not enough unique indices
        }
        return randomIndices;
    }

    public long[][] getPointQueries() {
        return pointQueries;
    }

    public int[] getPointQueryAnswers() {
        return pointQueryAnswers;
    }

    public int[] getPointQueriesNumAttrs() {
        return pointQueriesNumAttrs;
    }

    public int[] getPointQueryBinNumber() {
        return pointQueryBinNumber;
    }

    public int[] getPointQueryUnion() {
        return pointQueryUnion;
    }

}
