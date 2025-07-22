package org;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MixInMemory {

//    ArrayList<long[]> allRecords;
    ArrayList<Integer> allRecordsId; // Not used, but kept for reference
//    boolean[] hasBeenSeen;
    HashSet<Integer> seen;

    public MixInMemory(Config config) throws IOException {
        double sizeFactor = config.sizeFactor;
        String synthRootFolderName = config.readFolder + "input/synthFromDisk/" + sizeFactor;
        System.out.println("synthRootFolder: " + synthRootFolderName);
//        synthRootFolderName = synthRootFolderName.replace("./", ""); // Remove leading "./" if present

        File synthRootFolder = new File(synthRootFolderName);
        if (!synthRootFolder.exists() || !synthRootFolder.isDirectory()) {
            throw new IOException("Synth folder does not exist: " + synthRootFolder.getAbsolutePath());
        }

        // Pattern to detect shuffled residu file and extract suffix
        Pattern pattern = Pattern.compile("residu(.*)\\.csv");

        // Recursively process subfolders
        processFolderRecursively(synthRootFolder, pattern);


    }

    private void processFolderRecursively(File folder, Pattern pattern) throws IOException {
        System.out.println("Processing " + folder.getAbsolutePath());
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
            System.out.printf("Processing folder: " + folder.getAbsolutePath());
            System.out.printf("Residu: " + residuFile);
            if (hasInserts) System.out.println("Inserts: " + insertFile);

            // Create final stream file in parent folder
            String finalStreamFile = new File(folder.getParent(), "final_stream_spread_out" + suffix + ".csv").getAbsolutePath();
            finalStreamFile = finalStreamFile.replace("./", ""); // Remove leading "./" if present
            System.out.println("Final stream file: " + finalStreamFile);
            mixFiles(residuFile, hasInserts ? insertFile : null, finalStreamFile);
        }
    }

    private void mixFiles(String residuFile, String insertFile, String finalStreamFile) throws IOException {
        // Estimate initial size to reduce resizing (arbitrary reasonable defaults)
//        this.allRecords = new ArrayList<>(100_000);
        this.allRecordsId = new ArrayList<>(100_000); // Not used, but kept for reference
        ArrayList<int[]> inserts = new ArrayList<>();
        ArrayList<int[]> residu = new ArrayList<>();

        if (insertFile != null) {
            readDataset(insertFile, inserts, true);
            seen = new HashSet<>(inserts.size()); // Load factor consideration
            putInList(allRecordsId, inserts);
            putInList(allRecordsId, inserts);
        }

        int maxResiduId = readDataset(residuFile, residu, false);
        putInList(allRecordsId, residu);

        Collections.shuffle(allRecordsId);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(finalStreamFile))) {
            // Static header - no need to build this per iteration
            writer.write("id,attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9,sign\n");

            StringBuilder sb = new StringBuilder(256); // Reuse the same StringBuilder
            for (int rid : allRecordsId) {
                int[] record;
                if (rid <= maxResiduId) {
                    record = residu.get(rid);
                } else {
                    record = inserts.get(rid - maxResiduId - 1); // Adjust index for inserts
                }
                //test:
                if (record[0] != rid) {
                    throw new IllegalStateException("Record ID mismatch: expected " + rid + ", got " + record[0]);
                }

                sb.setLength(0); // Reset the StringBuilder

                for (int i = 0; i < record.length - 1; i++) {
                    sb.append(record[i]).append(',');
                }

                long sign = record[record.length - 1];
                if (sign == -2) {
                    sb.append("1");
                } else if (sign == -3) {
                    if (seen.add(rid)) { // add returns true if id was not present
                        sb.append("1");
                    } else {
                        sb.append("-1");
                        if (!seen.remove(rid)) {
                            throw new IllegalStateException("ID " + rid + " was not in seen set but was marked as delete.");
                        } // Remove from seen if it was already present
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

    private void putInList(ArrayList<Integer> allRecordsId, ArrayList<int[]> residu) {
        for (int[] record : residu) {
            if (record.length < 1) {
                throw new IllegalArgumentException("Record must have at least one element (ID).");
            }
            allRecordsId.add(record[0]); // Assuming ID is the first element
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



    public int readDataset(String filePath, ArrayList<int[]> dataset, boolean isNoise) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        int maxId = 0; // Track max ID for inserts
        reader.readLine(); // Skip header line
        String line;
        boolean firstIsSet = false; // Track if first ID is set
        int[] dataPoint = new int[0];
        while ((line = reader.readLine()) != null) {
            int length = line.length();
            int commaCount = 0;
            for (int i = 0; i < length; i++) {
                if (line.charAt(i) == ',') commaCount++;
            }

            if (!firstIsSet) {
                dataPoint = new int[commaCount + 1]; // Last column replaced by -2/-3
            }

            int partStart = 0;
            int partIndex = 0;
            for (int i = 0; i < length; i++) {
                if (i == length - 1 || line.charAt(i) == ',') {
//                    int partEnd = (line.charAt(i) == ',') ? i : i + 1;
                    if (partIndex < commaCount) { // Ignore last column
                        dataPoint[partIndex] = parseIntFast(line, partStart, i);
                        partIndex++;
                    }
                    partStart = i + 1;
                }
            }
//
//            for (int i = 0; i < length; i++) {
//                if (line.charAt(i) == ',' || i == length - 1) {
//                    int partEnd = (line.charAt(i) == ',') ? i : i + 1;
//                    if (partIndex < commas) { // Skip last column (sign replaced)
//                        String numberStr = line.substring(partStart, partEnd);
//                        dataPoint[partIndex] = Long.parseLong(numberStr);
//                        partIndex++;
//                    }
//                    partStart = i + 1;
//                }
//            }

            // Set last element manually based on isNoise
            dataPoint[dataPoint.length - 1] = isNoise ? -3 : -2;
            if (!isNoise && dataPoint[0] > maxId) {
                maxId = (int) dataPoint[0]; // Update max ID for residu
            }

            dataset.add(dataPoint);
        }
        reader.close();
        return maxId;
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

    private long parseLongFast(CharSequence s, int start, int end) {
        long result = 0;
        boolean negative = false;
        int i = start;

        if (s.charAt(i) == '-') {
            negative = true;
            i++;
        }

        while (i < end) {
            result = result * 10 + (s.charAt(i++) - '0');
        }

        return negative ? -result : result;
    }

    private int parseIntFast(CharSequence s, int start, int end) {
        int result = 0;
        boolean negative = false;
        int i = start;

        if (s.charAt(i) == '-') {
            negative = true;
            i++;
        }

        while (i < end) {
            result = result * 10 + (s.charAt(i++) - '0');
        }

        return negative ? -result : result;
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
