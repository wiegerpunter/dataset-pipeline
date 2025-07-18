package org;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;

public class CreateDatasetAndQueries {

    public static void main(String[] args) throws IOException {
        // This is a placeholder for the main method.
        // You can implement dataset reading logic here.
        // Implement your test logic here
        String jsonFilePath = args[0];
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(new File(jsonFilePath), Config.class);

        SyntheticDataset residu = new SyntheticDataset(config);
        for (double p : config.perc) {
            System.out.printf("Generating dataset with %.1f%% noise", p * 100);
            residu.synthDevDataGenerator(p, config.sizeFactor, config.zipfAlpha);
            if (p == 0) {
                System.out.println("Generating queries for dataset with no noise");
                residu.synthDevQueryGenerator(p, config.sizeFactor, config.zipfAlpha);
            }
        }


    }
}
