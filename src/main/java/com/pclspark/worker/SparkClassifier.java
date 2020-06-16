package com.pclspark.worker;

import com.pclspark.model.PointFeature;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.SparkSession;
import java.util.*;

public class SparkClassifier
{
    public static void randomForestClassifier()
    {
        SparkConf conf = new SparkConf()
            .set("spark.cassandra.connection.host", "192.168.29.100")
            .setJars(new String[]
                         {
                             "/home/research/Research/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
                             "/home/research/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                             "/home/research/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                         })
            .setMaster("spark://192.168.29.110:7077")
            .setAppName("PCL_PROCESSOR_CASSANDRA_SPACE_SPLIT");

        SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()
            .textFile("/home/research/dataset/feature.txt", 3)
            .toJavaRDD();

        JavaRDD<LabeledPoint> pointCloudFeatures = input
            .mapPartitions(new FlatMapFunction<Iterator<String>, LabeledPoint>()
            {
                @Override
                public Iterator<LabeledPoint> call(Iterator<String> stringIterator) throws Exception
                {
                    List<LabeledPoint> pointFeatures = new ArrayList<>();
                    while (stringIterator.hasNext())
                    {
                        String[] str = stringIterator.next().split(",");
                        float cl = Float.parseFloat(str[0].trim());
                        float cp = Float.parseFloat(str[0].trim());
                        float cs = Float.parseFloat(str[0].trim());
                        float anisotropy = Float.parseFloat(str[0].trim());
                        float changeofcurvature = Float.parseFloat(str[0].trim());;
                        float eigenentropy = Float.parseFloat(str[0].trim());;
                        float omnivariance = Float.parseFloat(str[0].trim());
                        int label = Integer.parseInt(str[0].trim());
                        PointFeature pointFeature =
                            new PointFeature(cl,cp,cs,omnivariance,
                                             anisotropy,eigenentropy,
                                             changeofcurvature, label);
                        pointFeatures.add(new LabeledPoint(label, Vectors.dense(pointFeature.getFeatures())));
                    }
                    return pointFeatures.iterator();
                }
            });

            JavaRDD<LabeledPoint>[] splits = pointCloudFeatures.randomSplit(new double[]{0.7, 0.3}, 12345);
            JavaRDD<LabeledPoint> trainingData = splits[0];
            JavaRDD<LabeledPoint> testData = splits[1];

            final int numClasses = 8;
            final Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
            final int numTrees = 10; // Use more in practice.
            final String featureSubsetStrategy = "auto"; // Let the algorithm choose feature subset strategy.
            final String impurity = "gini";
            final int maxDepth = 30;
            final int maxBins = 40;
            final int seed = 12345;

            /*
            final RandomForestModel model = RandomForest
                .trainClassifier(
                    trainingData,
                    numClasses,
                    categoricalFeaturesInfo,
                    numTrees,
                    featureSubsetStrategy,
                    impurity,
                    maxDepth,
                    maxBins,
                    seed);

             */
    }
}