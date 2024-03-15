package org.example;

import java.util.ArrayList;
import java.util.List;

public class FeatureVector {
    private List<Double> coordinates;

    private static double threshold;

    public FeatureVector() {
        this.coordinates = new ArrayList<>();
    }

    public FeatureVector(String coordinatesString, int leaveBehind) {
        this.coordinates = new ArrayList<>();
        extractCoordinates(coordinatesString, leaveBehind);
    }

    private void extractCoordinates(String coordinatesString, int leaveBehind) {
        String[] splitCoordinates = coordinatesString.split(",");
        for (int i = 0; i < splitCoordinates.length - leaveBehind; i++) {
            this.coordinates.add(Double.valueOf(splitCoordinates[i]));
        }
    }

    public List<Double> getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(List<Double> coordinates) {
        this.coordinates = coordinates;
    }

    public static void setThreshold(double thres) {
        threshold = thres;
    }
    

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<Double> coordinates = this.coordinates;

        for (int i = 0; i < coordinates.size(); i++) {
            sb.append(coordinates.get(i));
            if (i != coordinates.size() - 1) {
                sb.append(",");
            }
        }

        return sb.toString();
    }
}
