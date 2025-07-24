package org;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MixInMemoryDB {
    Connection conn;
    ArrayList<Integer> allRecordsId;
    HashSet<Integer> seen;
    double sizeFactor;

    public MixInMemoryDB(Config config) throws IOException, SQLException {
        sizeFactor = config.sizeFactor;
        conn = DriverManager.getConnection("jdbc:duckdb:" + config.readFolder + "tmp/mix_in_memory.db");
        String synthRootFolderName = config.readFolder + "input/data/synthFromDisk/" + sizeFactor + "/9.0";
        System.out.println("synthRootFolder: " + synthRootFolderName);

        File synthRootFolder = new File(synthRootFolderName);
        if (!synthRootFolder.exists() || !synthRootFolder.isDirectory()) {
            throw new IOException("Synth folder does not exist: " + synthRootFolder.getAbsolutePath());
        }

        Pattern pattern = Pattern.compile("residu(.*)\\.csv");
        processFolderRecursively(synthRootFolder, pattern);
    }

    private void processFolderRecursively(File folder, Pattern pattern) throws IOException, SQLException {
        System.out.println("Processing " + folder.getAbsolutePath());
        File[] files = folder.listFiles();
        if (files == null) return;

        boolean foundResidu = false;
        String suffix = null;
        String residuFile = null;
        String insertFile = null;

        for (File file : files) {
            if (file.isDirectory()) {
                processFolderRecursively(file, pattern);
            } else {
                Matcher matcher = pattern.matcher(file.getName());
                if (matcher.matches()) {
                    suffix = matcher.group(1);
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
            double perc = 0.0;
            if (hasInserts) {
                perc = Double.parseDouble(insertFile.split("1.3_0_")[1].split(".csv")[0]);
            }

            String finalStreamFile = new File(folder.getParent(), "final_stream_spread_out" + suffix + ".csv").getAbsolutePath();
            finalStreamFile = finalStreamFile.replace("./", "");
            mixFiles(residuFile, hasInserts ? insertFile : null, finalStreamFile, perc);
        }
    }

    private void mixFiles(String residuFile, String insertFile, String finalStreamFile, double perc) throws IOException, SQLException {
        int insertSize = (int) (perc * Math.pow(2, sizeFactor));
        int insertRecordSize = 11;
        this.allRecordsId = new ArrayList<>((int) (2 * insertSize));

        if (insertFile != null) {
            seen = new HashSet<>(100);
            loadInsertsIntoDuckDB(insertFile);
            putInListFromDB(allRecordsId);
            allRecordsId.addAll(allRecordsId);
        }

        ArrayList<int[]> residu = new ArrayList<>();

        int maxResiduId = readDataset(residuFile, residu, false);
        putInList(allRecordsId, residu);


        Collections.shuffle(allRecordsId);
        prepareStatement();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(finalStreamFile))) {
            writer.write("id,attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9,sign\n");
            StringBuilder sb = new StringBuilder(256);

            int progress = 0;
            for (int rid : allRecordsId) {
                progress+=1;
                if (progress % 10000 == 0) {
                    System.out.printf("\rProcessed %d / %d records.", progress, allRecordsId.size());
                }
                int[] record;
                if (rid <= maxResiduId) {
                    record = residu.get(rid);
                    if (record[0] != rid + 1) {
                        throw new IllegalStateException("Record ID mismatch: expected " + (rid + 1) + ", got " + record[0]);
                    }
                } else {
                    record = fetchInsertRecordFromDB(rid);
                    if (record[0] != rid) {
                        throw new IllegalStateException("Record ID mismatch: expected " + rid + ", got " + record[0]);
                    }
                }

                sb.setLength(0);
                for (int i = 0; i < record.length - 1; i++) {
                    sb.append(record[i]).append(',');
                }

                long sign = record[record.length - 1];
                if (sign == -2) {
                    sb.append("1");
                } else if (sign == 1) {
                    if (seen.add(rid)) {
                        sb.append("1");
                    } else {
                        sb.append("-1");
                        if (!seen.remove(rid)) {
                            throw new IllegalStateException("ID " + rid + " was not in seen set but was marked as delete.");
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected sign value: " + sign);
                }

                writer.write(sb.toString());
                writer.newLine();
            }

            if (seen != null && !seen.isEmpty()) {
                throw new IllegalStateException("Seen set should be empty after processing all records.");
            }
        }
    }

    private void loadInsertsIntoDuckDB(String insertFile) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS inserts (" +
                    "id INTEGER, attr1 INTEGER, attr2 INTEGER, attr3 INTEGER, attr4 INTEGER, " +
                    "attr5 INTEGER, attr6 INTEGER, attr7 INTEGER, attr8 INTEGER, attr9 INTEGER, " +
                    "sign INTEGER)");
            stmt.execute("DELETE FROM inserts");
            stmt.execute("DROP INDEX IF EXISTS idx_inserts_id");
            stmt.execute(String.format("COPY inserts FROM '%s' (AUTO_DETECT TRUE, HEADER FALSE)", insertFile));

            stmt.execute("CREATE INDEX idx_inserts_id ON inserts (id)");
        }
    }

    private PreparedStatement fetchInsertStmt;

    private void prepareStatement() throws SQLException {
        String sql = "SELECT id, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr9, sign FROM inserts WHERE id = ?";
        this.fetchInsertStmt = conn.prepareStatement(sql);
    }

    private int[] fetchInsertRecordFromDB(int id) throws SQLException {
        fetchInsertStmt.setInt(1, id);
        try (ResultSet rs = fetchInsertStmt.executeQuery()) {
            if (rs.next()) {
                int[] record = new int[11];
                for (int i = 0; i < 11; i++) {
                    record[i] = rs.getInt(i + 1);
                }
                return record;
            } else {
                throw new IllegalArgumentException("Insert ID " + id + " not found in DuckDB.");
            }
        }
    }

//
//    private int[] fetchInsertRecordFromDB(int id) throws SQLException {
//        String sql = "SELECT id, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr9, sign FROM inserts WHERE id = ?";
//        try (PreparedStatement ps = conn.prepareStatement(sql)) {
//            ps.setInt(1, id);
//            try (ResultSet rs = ps.executeQuery()) {
//                if (rs.next()) {
//                    int[] record = new int[11];
//                    for (int i = 0; i < 11; i++) {
//                        record[i] = rs.getInt(i + 1);
//                    }
//                    return record;
//                } else {
//                    throw new IllegalArgumentException("Insert ID " + id + " not found in DuckDB.");
//                }
//            }
//        }
//    }

    private void putInList(ArrayList<Integer> allRecordsId, ArrayList<int[]> residu) {
        for (int r = 0; r < residu.size(); r++) {
            allRecordsId.add(r);
        }
    }

    private void putInListFromDB(ArrayList<Integer> allRecordsId) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM inserts")) {
            while (rs.next()) {
                allRecordsId.add(rs.getInt(1));
            }
        }
    }

    public int readDataset(String filePath, ArrayList<int[]> dataset, boolean isNoise) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        int maxId = 0;
        reader.readLine();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            int[] dataPoint = new int[parts.length];
            for (int i = 0; i < parts.length - 1; i++) {
                dataPoint[i] = Integer.parseInt(parts[i]);
            }
            dataPoint[dataPoint.length - 1] = isNoise ? -3 : -2;
            if (!isNoise && dataPoint[0] > maxId) {
                maxId = dataPoint[0];
            }
            dataset.add(dataPoint);
        }
        reader.close();
        return maxId;
    }

    public static void main(String[] args) throws IOException, SQLException {
        String jsonFilePath = args[0];
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(new File(jsonFilePath), Config.class);

        try {
            new MixInMemoryDB(config);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}