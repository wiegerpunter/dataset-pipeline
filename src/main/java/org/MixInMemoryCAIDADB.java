package org;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MixInMemoryCAIDADB {
    Connection conn;
    ArrayList<Integer> allRecordsId;
    HashSet<Integer> seen;
    int sizeFactor;
    private final Map<InsertRef, String[]> insertRecordCache = new HashMap<>();
    private final int BATCH_SIZE = 1;

    static class InsertRef {
        int rid;
        int sign;

        InsertRef(int rid, int sign) {
            this.sign = sign;
            this.rid = rid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InsertRef other)) return false;
            return rid == other.rid && sign == other.sign;  // include sign!
        }

        @Override
        public int hashCode() {
            return Objects.hash(rid, sign);
        }
    }



    public MixInMemoryCAIDADB(Config config) throws IOException, SQLException {
        sizeFactor = config.sizeFactor;
        conn = DriverManager.getConnection("jdbc:duckdb:" + config.readFolder + "tmp/mix_in_memory.db");
        String synthRootFolderName = config.readFolder + "input/data/CAIDA/" + sizeFactor;
        System.out.println("synthRootFolder: " + synthRootFolderName);

        File synthRootFolder = new File(synthRootFolderName);
        if (!synthRootFolder.exists() || !synthRootFolder.isDirectory()) {
            throw new IOException("Synth folder does not exist: " + synthRootFolder.getAbsolutePath());
        }

        Pattern pattern = Pattern.compile("residu\\.csv");
        processFolderRecursively(synthRootFolder, pattern);
    }

    private void processFolderRecursively(File folder, Pattern pattern) throws IOException, SQLException {
        System.out.println("Processing " + folder.getAbsolutePath());
        File[] files = folder.listFiles();
        if (files == null) return;

        String residuFile = null;

        // First find the residu file
        for (File file : files) {
            Matcher matcher = pattern.matcher(file.getName());
            if (matcher.matches()) {
                residuFile = file.getAbsolutePath();
                break;
            }
        }

        if (residuFile == null) {
            throw new FileNotFoundException("residu.csv not found in folder: " + folder.getAbsolutePath());
        }

        // Look for insert folders (like 0.33/, 1.0/, etc.)
        for (File subfolder : files) {
            if (subfolder.isDirectory()) {
                String percName = subfolder.getName();
                File insertFile = new File(subfolder, percName + ".csv");

                if (insertFile.exists()) {
                    double perc = Double.parseDouble(percName);
                    String finalStreamFile = new File(subfolder, "final_stream_" + percName + ".csv").getAbsolutePath();
                    mixFiles(residuFile, insertFile.getAbsolutePath(), finalStreamFile, perc);
                }
            }
        }
    }


    private void mixFiles(String residuFile, String insertFile, String finalStreamFile, double perc) throws IOException, SQLException {
        this.allRecordsId = new ArrayList<>((10000));

        if (insertFile != null) {
            seen = new HashSet<>((1000));
            loadInsertsIntoDuckDB(insertFile);
            putInListFromDB(allRecordsId);
            allRecordsId.addAll(allRecordsId);
        }

        ArrayList<String[]> residu = new ArrayList<>();

        int maxResiduId = readDataset(residuFile, residu, false);
        putInList(allRecordsId, residu);


        Collections.shuffle(allRecordsId, new Random(42)); // Shuffle to ensure randomness in the final stream
        prepareStatement();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(finalStreamFile))) {
            writer.write("id,frametime,ipsrc,ipsrcnet,ipsrchost,ipdst,ipdstnet,ipdsthost,ipproto,sign\n");
            StringBuilder sb = new StringBuilder(256);

            int progress = 0;
            List<InsertRef> processingBuffer = new ArrayList<>(BATCH_SIZE);
            List<InsertRef> insertIdsToFetch = new ArrayList<>();

            for (int i = 0; i < allRecordsId.size(); i++) {
                int rid = allRecordsId.get(i);
                InsertRef insertRef = new InsertRef(rid, 1);
                if (rid >= maxResiduId ) {
                    if (seen.add(rid)) {
                        insertRef.sign = 1;
                    } else {
                        if (!seen.remove(rid)) {
                            throw new IllegalStateException("ID " + rid + " was not in seen set but was marked as delete.");
                        }
                        insertRef.sign = -1; // If already seen, mark as delete
                    }
                }
                processingBuffer.add(insertRef);

                if (rid > maxResiduId) {
                    insertIdsToFetch.add(insertRef);
                }

                if (processingBuffer.size() >= BATCH_SIZE || i == allRecordsId.size() - 1) {
                    // Fetch all needed insert records
                    if (!insertIdsToFetch.isEmpty()) {
                        batchFetchInsertRecords(insertIdsToFetch);
                        insertIdsToFetch.clear();
                    }

                    // Process everything in exact order
                    for (InsertRef ridInBuffer : processingBuffer) {
                        if (progress % 1000 == 0) {
                            System.out.printf("\rProcessed %d / %d records.", progress, allRecordsId.size());
                        }
                        progress++;

                        if (ridInBuffer.rid <= maxResiduId) {
                            writeResiduRecord(writer, sb, residu, ridInBuffer.rid);
                        } else {
                            writeInsertRecord(writer, sb, ridInBuffer);
                        }
                    }
                    processingBuffer.clear();
                }
            }

            if (!seen.isEmpty()) {
                throw new IllegalStateException("Seen set should be empty after processing all records.");
            }
            if (!insertRecordCache.isEmpty()) {
                throw new IllegalStateException("Insert record cache should be empty after processing all records.");
            }
        }

        File countFile = new File(new File(finalStreamFile).getParent(), "count.txt");
        try (BufferedWriter countWriter = new BufferedWriter(new FileWriter(countFile))) {
            countWriter.write(String.valueOf(allRecordsId.size()));
        }


    }

    private void loadInsertsIntoDuckDB(String insertFile) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS inserts");
            stmt.execute("CREATE TABLE inserts (" +
                    "id INTEGER, " +
                    "frametime BIGINT, " +
                    "ipsrc BIGINT, " +
                    "ipsrcnet BIGINT, " +
                    "ipsrchost BIGINT, " +
                    "ipdst BIGINT, " +
                    "ipdstnet BIGINT, " +
                    "ipdsthost BIGINT, " +
                    "ipproto INTEGER, " +
                    "sign INTEGER" +
                    ")");
//            stmt.execute("SELECT * FROM read_csv_auto('/home/wieger/omni-deletes/input/data/tpc-ds/1000/3.0/3.0.csv', delim=',', header=true)");
//            try (ResultSet rs = stmt.executeQuery("SELECT * FROM read_csv_auto('" + insertFile + "', delim=',', header=true)")) {
//                while (rs.next()) {
////                    System.out.println(rs.getString(1)); // or loop over columns
//                    int id = rs.getInt("id");
//                    long frametime = rs.getLong("frametime");
//                    Long ipsrc = rs.getLong("ipsrc");
//                    Long ipsrcnet = rs.getLong("ipsrcnet");
//                    Long ipsrchost = rs.getLong("ipsrchost");
//                    Long ipdst = rs.getLong("ipdst");
//                    Long ipdstnet = rs.getLong("ipdstnet");
//                    Long ipdsthost = rs.getLong("ipdsthost");
//                    int ipproto = rs.getInt("ipproto");
//                    int sign = rs.getInt("sign");
//                    String insertSQL = "INSERT INTO inserts (id, frametime, ipsrc , ipsrcnet, ipsrchost, ipdst, ipdstnet, ipdsthost, ipproto, sign) " +
//                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//                    try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
//                        insertStmt.setInt(1, id);
//                        insertStmt.setLong(2, frametime);
//                        insertStmt.setObject(3, ipsrc != null ? ipsrc : null);
//                        insertStmt.setObject(4, ipsrcnet != null ? ipsrcnet : null);
//                        insertStmt.setObject(5, ipsrchost != null ? ipsrchost : null);
//                        insertStmt.setObject(6, ipdst != null ? ipdst : null);
//                        insertStmt.setObject(7, ipdstnet != null ? ipdstnet : null);
//                        insertStmt.setObject(8, ipdsthost != null ? ipdsthost : null);
//                        insertStmt.setInt(9, ipproto);
//                        insertStmt.setInt(10, sign);
//                        insertStmt.executeUpdate();
//                    }
//                }

//
//
//            stmt.execute("DROP TABLE IF EXISTS inserts");
//            stmt.execute("CREATE TABLE inserts (" +
//                    "id INTEGER, " +
//                    "c_salutation VARCHAR, " +
//                    "c_first_name VARCHAR, " +
//                    "c_last_name VARCHAR, " +
//                    "c_birth_country VARCHAR, " +
//                    "c_current_addr_sk INTEGER, " +
//                    "sign INTEGER" +
//                    ")");
            stmt.execute(String.format("COPY inserts FROM '%s' (DELIMITER ',', HEADER TRUE, NULL '', STRICT_MODE FALSE)", insertFile));
            stmt.execute("CREATE INDEX idx_inserts_id ON inserts (id)");
        }
    }

    private PreparedStatement fetchInsertStmt;

    private String[] fetchInsertRecordFromDB(int id) throws SQLException {
        fetchInsertStmt.setInt(1, id);
        try (ResultSet rs = fetchInsertStmt.executeQuery()) {
            if (rs.next()) {
                String[] record = new String[7];
                for (int i = 0; i < 7; i++) {
                    record[i] = rs.getString(i + 1);
                }
                return record;
            } else {
                throw new IllegalArgumentException("Insert ID " + id + " not found in DuckDB.");
            }
        }
    }

    private void prepareStatement() throws SQLException {
        String sql = "SELECT id,frametime,ipsrc,ipsrcnet,ipsrchost,ipdst,ipdstnet,ipdsthost,ipproto,sign FROM inserts WHERE id = ?";
        this.fetchInsertStmt = conn.prepareStatement(sql);
    }


    private void batchFetchInsertRecords(List<InsertRef> ids) throws SQLException {
        Set<Integer> uniqueIds = new HashSet<>();
        for (InsertRef ref : ids) {
            uniqueIds.add(ref.rid);
        }

        String placeholders = String.join(",", Collections.nCopies(uniqueIds.size(), "?"));
        String sql = "SELECT id,frametime,ipsrc,ipsrcnet,ipsrchost,ipdst,ipdstnet,ipdsthost,ipproto,sign " +
                "FROM inserts WHERE id IN (" + placeholders + ")";

        Map<Integer, String[]> fetchedRecords = new HashMap<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int i = 1;
            for (int uid : uniqueIds) {
                stmt.setInt(i++, uid);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String[] record = new String[7];
                    for (int j = 0; j < 7; j++) {
                        record[j] = rs.getString(j + 1);
                    }
                    fetchedRecords.put(rs.getInt(1), record);
                }
            }
        }

        for (InsertRef ref : ids) {
            String[] record = fetchedRecords.get(ref.rid);
            if (record == null) {
                throw new IllegalStateException("Missing insert record for ID " + ref.rid);
            }
            insertRecordCache.put(ref, record.clone());
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

    private void putInList(ArrayList<Integer> allRecordsId, ArrayList<String[]> residu) {
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

    public int readDataset(String filePath, ArrayList<String[]> dataset, boolean isNoise) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        int maxId = 0;
        reader.readLine();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            String[] dataPoint = new String[parts.length];
            for (int i = 0; i < parts.length - 1; i++) {
                dataPoint[i] = parts[i];
            }
            dataPoint[dataPoint.length - 1] = String.valueOf(isNoise ? -3 : -2);
            if (!isNoise && Integer.parseInt(dataPoint[0]) > maxId) {
                maxId = Integer.parseInt(dataPoint[0]);
            }
            dataset.add(dataPoint);
        }
        reader.close();
        return maxId;
    }

    private void writeResiduRecord(BufferedWriter writer, StringBuilder sb, List<String[]> residu, int rid) throws IOException {
        String[] record = residu.get(rid);
        if (Integer.parseInt(record[0]) != rid + 1) {
            throw new IllegalStateException("Record ID mismatch: expected " + (rid + 1) + ", got " + record[0]);
        }
        writeRecord(writer, sb, record, 1);
    }

    private void writeInsertRecord(BufferedWriter writer, StringBuilder sb, InsertRef insertRef) throws IOException {
        String[] record = insertRecordCache.remove(insertRef);
        if (record == null) {
            throw new IllegalStateException("Insert record not found in cache for ID: " + insertRef.rid +
                    " with sign: " + insertRef.sign);
        }
        if (Integer.parseInt(record[0]) != insertRef.rid) {
            throw new IllegalStateException("Record ID mismatch: expected " + insertRef.rid + ", got " + record[0]);
        }
        writeRecord(writer, sb, record, insertRef.sign);

    }

    private void writeRecord(BufferedWriter writer, StringBuilder sb, String[] record, int sign) throws IOException {
        sb.setLength(0);
        for (int i = 0; i < record.length - 1; i++) {
            sb.append(record[i]).append(',');
        }
        if (sign == 1) {
            sb.append("1");
        } else if (sign ==-1) {
            sb.append("-1");
        } else {
            throw new IllegalArgumentException("Unexpected sign value: " + sign);
        }

        writer.write(sb.toString());
        writer.newLine();
    }


    public static void main(String[] args) throws IOException, SQLException {
        String jsonFilePath = args[0];
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(new File(jsonFilePath), Config.class);

        try {
            new MixInMemoryCAIDADB(config);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}