package org.example;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {

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


    private static String extractCentroid(String line) {
        String[] parts = line.split("\t");
        return parts[1].split("#")[0] + ",centroid";
    }

    public static List<String> getCentroids(String filePath) {
        List<String> centroids = new ArrayList<>();
        List<String> data = loadDataFromFile(filePath);
        for (String line : data) {
            String centroid = extractCentroid(line);
            centroids.add(centroid);
        }

        return centroids;
    }

    public static boolean checkEqual(List<String> oldCentroids, List<String> newCentroids, double threshold) {
        for (int i = 0; i < oldCentroids.size(); i++) {
            FeatureVector oldCentroid = new FeatureVector(oldCentroids.get(i), 1);
            FeatureVector newCentroid = new FeatureVector(newCentroids.get(i), 1);
            if (! oldCentroid.checkEquals(newCentroid) )
                return false;
        }
        return true;
    }
}
