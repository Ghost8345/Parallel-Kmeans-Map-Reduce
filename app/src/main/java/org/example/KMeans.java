package org.example;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

    public static final char DELIMITER = '_';


    public static class KMeansMapper
            extends Mapper<Object, Text, Text, Text>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim(); 
            if (line.isEmpty()) {
                return;
            }

            Configuration conf = context.getConfiguration();
            int clustersNumber = Integer.parseInt(conf.get("clustersNumber"));
            List<String> centroidsAsStrings = new ArrayList<>();
            for (int i = 0; i < clustersNumber; i++) {
                centroidsAsStrings.add(conf.get("c"+i));
            }

            List<FeatureVector> centroids = new ArrayList<>();
            for (String centroidString : centroidsAsStrings) {
                FeatureVector centroid = new FeatureVector(centroidString, 1);
                centroids.add(centroid);
            }

            double minDistance = Double.MAX_VALUE;
            int minIndex = 0;

            FeatureVector fv = new FeatureVector(value.toString(),1);

            for (int i = 0; i < centroids.size(); i++) {
                double distance = fv.getDistanceFrom(centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    minIndex = i;
                }
            }

            context.write(new Text(centroids.get(minIndex).toString()), new Text(fv.toString()));
        }
    }

    public static class KMeansCombiner
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<Double> sum = new ArrayList<>(Collections.nCopies(key.toString().split(",").length, 0.0));
            StringBuilder sb = new StringBuilder();
            int count = 0;

            for (Text val : values) {
                FeatureVector fv = new FeatureVector(val.toString(),0);
                count++;
                sb.append(val).append(DELIMITER);
                List<Double> coordinates = fv.getCoordinates();
                for (int i = 0; i < coordinates.size(); i++) {
                    sum.set(i, sum.get(i) + coordinates.get(i));
                }
            }

            sb.append(count);

            FeatureVector sumVector = new FeatureVector();
            sumVector.setCoordinates(sum);

            sb.insert(0, DELIMITER).insert(0, sumVector.toString());


            context.write(key, new Text(sb.toString()));
        }

    }
    public static class KMeansReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            List<Double> mean = new ArrayList<>(Collections.nCopies(key.toString().split(",").length, 0.0));
            int count = 0;

            for (Text val : values) {

                String[] localCluster = val.toString().split(String.valueOf(DELIMITER));

                count += Integer.valueOf(localCluster[localCluster.length - 1]);
                for (int i = 1; i < localCluster.length - 1; i++) {
                    sb.append(localCluster[i]);
                    if (i != localCluster.length - 2) {
                        sb.append(DELIMITER);
                    }
                }

                FeatureVector fv = new FeatureVector(localCluster[0],0);
                List<Double> coordinates = fv.getCoordinates();
                for (int i = 0; i < coordinates.size(); i++) {
                    mean.set(i, mean.get(i) + coordinates.get(i));
                }
            }

            for (int i = 0; i < mean.size(); i++) {
                mean.set(i, mean.get(i) / count);
            }


            FeatureVector meanVector= new FeatureVector();
            meanVector.setCoordinates(mean);

            sb.insert(0, DELIMITER).insert(0, meanVector.toString());
            context.write(key, new Text(sb.toString()));
        }

    }
    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: KMeans <input path> <output path> <num clusters>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[1]);

        final int  maxIterationsNum = 500;
        int iterationNum = 0;
        double threshold = 1e-7;
        FeatureVector.setThreshold(threshold);

        Path outputPath = new Path(args[2] + "/Iteration_" + iterationNum);

        int clustersNumber = Integer.parseInt(args[3]);

        List<String> centroids = Reader.getInitialCentroids(inputPath.toString() + "/iris.data", clustersNumber);

        long start = System.currentTimeMillis();

        for(int i = 0 ;i < maxIterationsNum; i++) {

            System.out.println("Iteration " + iterationNum + " Started");
            iterationNum++;
            Configuration conf = new Configuration();
            conf.set("clustersNumber", String.valueOf(clustersNumber));
            for (int j = 0; j < clustersNumber; j++) {
                conf.set("c" + j, centroids.get(j));
            }
            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);

            List<String> oldCentroids = new ArrayList<>(centroids);
            centroids = Reader.getCentroids(outputPath.toString() + "/part-r-00000", DELIMITER);
            

            if(checkEqual(oldCentroids,centroids,threshold)){
                break;
            }
            outputPath = new Path(args[2] + "/Iteration_" + iterationNum);
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Total time of the parallel KMeans is "+timeElapsed/1000+" seconds");
        System.out.println("Number of Iterations is: " + iterationNum);
        System.out.println("Final Centroids are : ");
        for (int i = 0; i < centroids.size(); i++) {
            System.out.println("c"+i + ": " + centroids.get(i));
        }
        

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