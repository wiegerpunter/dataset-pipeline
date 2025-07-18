package org;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MixInMemory {

    ArrayList<long[]> allRecords;
    boolean[] hasBeenSeen;
    HashSet<Long> seen;

    public MixInMemory(Config config) throws IOException {
        System.out.println(config.readFolder);
        double sizeFactor = config.sizeFactor;
        File synthRootFolder = new File(config.readFolder + "input/data/synthFromDisk/" + sizeFactor);
        if (!synthRootFolder.exists() || !synthRootFolder.isDirectory()) {
            throw new IOException("Synth folder does not exist: " + synthRootFolder.getAbsolutePath());
        }

        // Pattern to detect shuffled residu file and extract suffix
        Pattern pattern = Pattern.compile("residu(.*)\\.csv");

        // Recursively process subfolders
        processFolderRecursively(synthRootFolder, pattern);


    }

    private void processFolderRecursively(File folder, Pattern pattern) throws IOException {
        File[] files = folder.listFiles();
        if (files == null) return;

        boolean foundResidu = false;
        String suffix = null;
        String residuFile = null;
        String insertFile = null;

        for (File file : files) {
            if (file.isDirectory()) {
                processFolderRecursively(file, pattern);  // Recurse into subfolders
            } else {
                Matcher matcher = pattern.matcher(file.getName());
                if (matcher.matches()) {
                    suffix = matcher.group(1);  // Includes leading underscores
                    residuFile = file.getAbsolutePath();
                    insertFile = new File(folder, "noise_inserts" + suffix + ".csv").getAbsolutePath();
                    foundResidu = true;
                    break;
                }
            }
        }

        if (foundResidu) {
            if (!new File(residuFile).exists()) {
                throw new FileNotFoundException("Residu file missing in: " + folder.getAbsolutePath());
            }

            boolean hasInserts = new File(insertFile).exists();
            System.out.printf("\rProcessing folder: " + folder.getAbsolutePath());
            System.out.printf("\rResidu: " + residuFile);
            if (hasInserts) System.out.println("Inserts: " + insertFile);

            // Create final stream file in parent folder
            String finalStreamFile = new File(folder.getParent(), "final_stream_spread_out" + suffix + ".csv").getAbsolutePath();

            mixFiles(residuFile, hasInserts ? insertFile : null, finalStreamFile);
        }
    }

    private void mixFiles(String residuFile, String insertFile, String finalStreamFile) throws IOException {
        // Estimate initial size to reduce resizing (arbitrary reasonable defaults)
        this.allRecords = new ArrayList<>(100_000);
        ArrayList<long[]> inserts = new ArrayList<>();

        if (insertFile != null) {
            readDataset(insertFile, inserts, true);
            seen = new HashSet<>(inserts.size()); // Load factor consideration
            allRecords.addAll(inserts);
            allRecords.addAll(inserts); // Add noise twice
        }

        readDataset(residuFile, allRecords, false);
        Collections.shuffle(allRecords);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(finalStreamFile))) {
            // Static header - no need to build this per iteration
            writer.write("id,attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9,sign\n");

            StringBuilder sb = new StringBuilder(256); // Reuse the same StringBuilder
            for (long[] record : allRecords) {
                sb.setLength(0); // Reset the StringBuilder

                for (int i = 0; i < record.length - 1; i++) {
                    sb.append(record[i]).append(',');
                }

                long sign = record[record.length - 1];
                if (sign == -2) {
                    sb.append("1");
                } else if (sign == -3) {
                    long id = record[0];
                    if (seen.add(id)) { // add returns true if id was not present
                        sb.append("1");
                    } else {
                        sb.append("-1");
                        if (!seen.remove(id)) {
                            throw new IllegalStateException("ID " + id + " was not in seen set but was marked as delete.");
                        }; // Remove from seen if it was already present
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected sign value: " + sign);
                }

                writer.write(sb.toString());
                writer.newLine();
            }

            if (seen!= null && !seen.isEmpty()) {

                throw new IllegalStateException("Seen set should be empty after processing all records.");
            }

        }
    }


//    private void mixFiles(String residuFile, String insertFile, String finalStreamFile) throws IOException {
//        this.allRecords = new ArrayList<>();
//        if (insertFile != null) {
//            readDataset(insertFile, allRecords, true);
//            seen = new HashSet<>(); // Initialize seen set for noise inserts
//            hasBeenSeen = new boolean[allRecords.size()]; // set to false.
//            allRecords.addAll(allRecords); // add noise twice
//        }
//        readDataset(residuFile, allRecords, false);
//        Collections.shuffle(allRecords); // Shuffle the combined records
//
//        // Changing the isNoise, isResidu to isInsert, isDelete
//        int index = 0;
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter(finalStreamFile))) {
//            writer.write("id,attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9,sign\n"); // Header line
//            for (long[] record : allRecords) {
//                StringBuilder sb = new StringBuilder();
//                for (int i = 0; i < record.length - 1; i++) {
//                    sb.append(record[i]).append(",");
//                }
//                // if record[record.length - 1] == -2, it is insert for sure.
//                // if record[record.length - 1] == -3, check if record has been seen, then it is insert, else delete.
//                if (record[record.length - 1] == -2) {
//                    sb.append("1");
//                } else if (record[record.length - 1] == -3) {
//                    if (!seen.contains(record[0])) {
//                        seen.add(record[0]); // Mark as seen
//                        sb.append("1");
//                    } else {
//                        sb.append("-1");
//                    }
//                } else {
//                    throw new IllegalArgumentException("Unexpected sign value: " + record[record.length - 1]);
//                }
//                writer.write(sb.toString());
//                writer.newLine();
//            }
//        }
//    }



    public void readDataset(String filePath, ArrayList<long[]> dataset, boolean isNoise) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        reader.readLine(); // Skip header line
        String line;
        while ((line = reader.readLine()) != null) {
            int length = line.length();
            int partStart = 0;
            int partIndex = 0;

            // Count commas to know array size
            int commas = 0;
            for (int i = 0; i < length; i++) {
                if (line.charAt(i) == ',') commas++;
            }

            long[] dataPoint = new long[commas + 1]; // Last column replaced by -2/-3

            for (int i = 0; i < length; i++) {
                if (line.charAt(i) == ',' || i == length - 1) {
                    int partEnd = (line.charAt(i) == ',') ? i : i + 1;
                    if (partIndex < commas) { // Skip last column (sign replaced)
                        String numberStr = line.substring(partStart, partEnd);
                        dataPoint[partIndex] = Long.parseLong(numberStr);
                        partIndex++;
                    }
                    partStart = i + 1;
                }
            }

            // Set last element manually based on isNoise
            dataPoint[dataPoint.length - 1] = isNoise ? -3 : -2;

            dataset.add(dataPoint);
        }
        reader.close();
//        while (line != null) {
//            parts = line.split(",");
//            long[] dataPoint = new long[parts.length]; // don't need sign.
//            for (int i = 0; i < parts.length - 1; i++) {
//                dataPoint[i] = Long.parseLong(parts[i]);
//            }
//            if (isNoise) {
//                dataPoint[dataPoint.length - 1] = -3; // Set sign to -3 for noise inserts
//            } else {
//                dataPoint[dataPoint.length - 1] = -2; // Set sign to -2 for residu
//            }
//            dataset.add(dataPoint);
//            line = reader.readLine();
//        }
//        reader.close();
    }

    public static void main(String[] args) throws IOException {
        String jsonFilePath = args[0];
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(new File(jsonFilePath), Config.class);


        try {
            MixInMemory mixInMemory = new MixInMemory(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
