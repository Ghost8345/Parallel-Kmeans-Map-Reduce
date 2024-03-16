package org.example;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Reader {

    private static List<String> loadDataFromFile(String filePath) {
        List<String> data = new ArrayList<>();
        
        try {
            Path path = new Path(filePath);
            FileSystem fs = path.getFileSystem(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    
            String line;
            while ((line = br.readLine()) != null) {
                data.add(line);
            }
            br.close();
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }

        return data;
    }

    public static List<String> getInitialCentroids(String filePath, int k) {
        List<String> data = loadDataFromFile(filePath);
        if (data.isEmpty()) {
            System.out.println("No data loaded.");
            return new ArrayList<>();
        }

        List<String> initialCentroids = new ArrayList<>();
        Set<Integer> pickedIndices = new HashSet<>();
        Random random = new Random();

        for (int i = 0; i < k; i++) {
            int index = random.nextInt(data.size());
            while (pickedIndices.contains(index)) {
                index = random.nextInt(data.size());
            }
            initialCentroids.add(data.get(index));
            pickedIndices.add(index);
        }

        return initialCentroids;
    }


    private static String extractCentroid(String line, char delimiter) {
        String[] parts = line.split("\t");
        return parts[1].split(String.valueOf(delimiter))[0] + ",centroid";
    }

    public static List<String> getCentroids(String filePath, char delimiter) {
        List<String> centroids = new ArrayList<>();
        List<String> data = loadDataFromFile(filePath);
        for (String line : data) {
            String centroid = extractCentroid(line, delimiter);
            centroids.add(centroid);
        }

        return centroids;
    }

}
