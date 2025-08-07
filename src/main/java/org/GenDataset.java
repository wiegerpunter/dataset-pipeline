package org;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class GenDataset {

    public static void main(String[] args) throws IOException {
        // This is a placeholder for the main method.
        // You can implement dataset reading logic here.
        // Implement your test logic here
        String jsonFilePath = args[0];
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(new File(jsonFilePath), Config.class);

        SyntheticDatasetRef residu = new SyntheticDatasetRef(config);

        residu.synthDevDataGenerator(config.numZipfAttributes, config.sizeFactor, config.zipfAlpha);
    }
}
