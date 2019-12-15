import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class PclSpark
{
    public static  void main(String args[])
    {
       /*
        Morton3D morton = new Morton3D();
        System.out.println(morton.encode(0,0,0));
        System.out.println(morton.encode(2,0,0));
        System.out.println(morton.encode(2,0,2));
        System.out.println(morton.encode(0,2,0));
        System.out.println(morton.encode(0,0,2));
        System.out.println(morton.encode(0,2,2));
        System.out.println(morton.encode(2,2,2));
        System.out.println(morton.encode(2,2,0));

        int[] x = morton.decode(9920124610L);
        System.out.println(x[0]);
        System.out.println(x[1]);
        System.out.println(x[2]);
        */

        /*SparkConf conf = new SparkConf()
                .setJars(new String []{"/home/node-master/IdeaProjects/PCLSpark/build/libs/untitled-1.jar"})
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> input = spark.read().textFile("/home/hadoop/3d_4.txt").javaRDD();
        JavaRDD<Coordinate> coordinates = input.map((Function<String, Coordinate>) s ->
                {
                    String[] cord = s.split(" ");
                    Coordinate coordinate = new Coordinate(
                            Float.parseFloat(cord[1]),
                            Float.parseFloat(cord[2]),
                            Float.parseFloat(cord[3]));
                    return coordinate;
                });

        JavaRDD<Coordinate> sortedCoordinates = coordinates
                .sortBy(new Function<Coordinate, Long>()
        {
            @Override
            public Long call(Coordinate v1) throws Exception
            {
                return v1.getM();
            }
        }, true, 1);

        /*
        JavaPairRDD<Object, Object> pairRDD = sortedCoordinates.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Coordinate>, Object, Object>()
        {
            @Override
            public Iterator<Tuple2<Object, Object>> call(Iterator<Coordinate> coordinateIterator) throws Exception
            {
                return null;
            }
        });


        long pairCount = pairRDD.count();
        System.out.print(pairCount);
        */

        /*
        Encoder<Coordinate> encoders = Encoders.bean(Coordinate.class);
        Dataset<Row> dataset =   spark.createDataFrame(sortedCoordinates, Coordinate.class);
        dataset.createOrReplaceTempView("Coordinate");
        dataset.show(20);
        */

        /*
         SparkRadiusSearch search = new SparkRadiusSearch();
         //search.pointDistance();
        search.Init();
        search.RadiusSearchForAll(0.8);
         */

        SparkCassandra cassandraSpark = new SparkCassandra();
        //
        //cassandraSpark.Process();
        SparkCassandra.ProcessKNN();
    }
}