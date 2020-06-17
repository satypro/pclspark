package com.pclspark.worker;

import com.pclspark.model.PointFeature;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SparkClassifier
{
    public static void randomForestClassifier()
    {
        SparkConf conf = new SparkConf()
            .setJars(new String[]
                         {
                             "/home/research/Research/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar"
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
                        float cp = Float.parseFloat(str[1].trim());
                        float cs = Float.parseFloat(str[2].trim());
                        float anisotropy = Float.parseFloat(str[3].trim());
                        float changeofcurvature = Float.parseFloat(str[4].trim());;
                        float eigenentropy = Float.parseFloat(str[5].trim());;
                        float omnivariance = Float.parseFloat(str[6].trim());
                        int label = Integer.parseInt(str[7].trim());
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

            final int numClasses = 9;
            final Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
            final int numTrees = 10; // Use more in practice.
            final String featureSubsetStrategy = "auto"; // Let the algorithm choose feature subset strategy.
            final String impurity = "gini";
            final int maxDepth = 30;
            final int maxBins = 40;
            final int seed = 12345;

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


        // Evaluation-1: evaluate the model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<>(model.predict(p.features()), p.label());
                    }
                });

        Double testErr =
                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();
        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification forest model:\n" + model.toDebugString());

        // Evaluation-2: evaluate the model on test instances and compute the related performance measure statistics
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testData.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        System.out.println(metrics.confusionMatrix());
        System.out.println(metrics.confusionMatrix());
        double precision = metrics.precision(metrics.labels()[0]);
        double recall = metrics.recall(metrics.labels()[0]);
        double f_measure = metrics.fMeasure();
        double query_label = 8.0;
        double TP = metrics.truePositiveRate(query_label);
        double FP = metrics.falsePositiveRate(query_label);
        double WTP = metrics.weightedTruePositiveRate();
        double WFP =  metrics.weightedFalsePositiveRate();
        System.out.println("Precision = " + precision);
        System.out.println("Recall = " + recall);
        System.out.println("F-measure = " + f_measure);
        System.out.println("True Positive Rate = " + TP);
        System.out.println("False Positive Rate = " + FP);
        System.out.println("Weighted True Positive Rate = " + WTP);
        System.out.println("Weighted False Positive Rate = " + WFP);
    }
}