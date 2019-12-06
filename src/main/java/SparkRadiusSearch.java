import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class SparkRadiusSearch implements Serializable
{
    static JavaRDD<Coordinate> coordinates = null;
    static JavaRDD<String> input = null;

    public void Init()
    {
        SparkConf conf = new SparkConf()
                .setJars(new String []{"/home/hadoop/IdeaProjects/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar"})
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR_RADIUS_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        SparkRadiusSearch.input = spark.sparkContext()
                .textFile("/home/hadoop/sparkinput/3d_4.txt", 2)
                .toJavaRDD();

        SparkRadiusSearch.coordinates = input.map((Function<String, Coordinate>) s ->
        {
            String[] cord = s.split(" ");
            Coordinate coordinate = new Coordinate(
                    Float.parseFloat(cord[1]),
                    Float.parseFloat(cord[2]),
                    Float.parseFloat(cord[3]));
            return coordinate;
        });
    }

    public static List<Coordinate> RadiusSearch(JavaRDD<Coordinate> coordinates, Coordinate inputCord, double radius)
    {
        JavaRDD<Coordinate> filtered = coordinates.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, Coordinate>() {
            @Override
            public Iterator<Coordinate> call(Iterator<Coordinate> coordinateIterator) throws Exception
            {
                List<Coordinate> pointList = new ArrayList<>();
                while (coordinateIterator.hasNext())
                {
                    Coordinate coordinate = coordinateIterator.next();
                    float xdiff = (inputCord.getX() - coordinate.getX()) * (inputCord.getX() - coordinate.getX());
                    float ydiff = (inputCord.getY() - coordinate.getY()) * (inputCord.getY() - coordinate.getY());
                    float zdiff = (inputCord.getZ() - coordinate.getZ()) * (inputCord.getZ() - coordinate.getZ());

                    double distance = Math.sqrt(xdiff + ydiff +zdiff);
                    if (distance <= radius)
                    {
                        pointList.add(coordinate);
                    }
                };

                return pointList.iterator();
            }
        });

        return filtered.collect();
    }

    public JavaRDD<Coordinate> RadiusSearchv2(Coordinate inputCord, double radius)
    {
        SparkConf conf = new SparkConf()
                .setJars(new String []{"/home/hadoop/IdeaProjects/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar"})
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR_RADIUS_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()
                .textFile("/home/hadoop/sparkinput/3d_4.txt", 2)
                .toJavaRDD();

        JavaRDD<Coordinate> coordinates = input.map((Function<String, Coordinate>) s ->
        {
            String[] cord = s.split(" ");
            Coordinate coordinate = new Coordinate(
                    Float.parseFloat(cord[1]),
                    Float.parseFloat(cord[2]),
                    Float.parseFloat(cord[3]));
            return coordinate;
        });

        JavaRDD<Coordinate> filtered = coordinates.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, Coordinate>() {
            @Override
            public Iterator<Coordinate> call(Iterator<Coordinate> coordinateIterator) throws Exception
            {
                List<Coordinate> pointList = new ArrayList<>();
                while (coordinateIterator.hasNext())
                {
                    Coordinate coordinate = coordinateIterator.next();
                    float xdiff = (inputCord.getX() - coordinate.getX()) * (inputCord.getX() - coordinate.getX());
                    float ydiff = (inputCord.getY() - coordinate.getY()) * (inputCord.getY() - coordinate.getY());
                    float zdiff = (inputCord.getZ() - coordinate.getZ()) * (inputCord.getZ() - coordinate.getZ());

                    double distance = Math.sqrt(xdiff + ydiff +zdiff);
                    if (distance <= radius)
                    {
                        pointList.add(coordinate);
                    }
                };

                return pointList.iterator();
            }
        });

        return filtered;
    }

    public void RadiusSearchForAll(double radius)
    {
        long count = coordinates.count();

        // Partition as per the comfortable Batch Size

        List<Coordinate> testPoints = SparkRadiusSearch.coordinates.top(10);

        for (Coordinate inputCord: testPoints)
        {
            JavaPairRDD<Coordinate, List<Coordinate>> cList = coordinates.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Coordinate>, Coordinate, List<Coordinate>>()
            {
                @Override
                public Iterator<Tuple2<Coordinate, List<Coordinate>>> call(Iterator<Coordinate> coordinateIterator) throws Exception
                {
                    List<Coordinate> pointList = new ArrayList<>();
                    List<Tuple2<Coordinate, List<Coordinate>>> tupleList = new ArrayList<Tuple2<Coordinate, List<Coordinate>>>();
                    while (coordinateIterator.hasNext())
                    {
                        Coordinate coordinate = coordinateIterator.next();
                        float xdiff = (inputCord.getX() - coordinate.getX()) * (inputCord.getX() - coordinate.getX());
                        float ydiff = (inputCord.getY() - coordinate.getY()) * (inputCord.getY() - coordinate.getY());
                        float zdiff = (inputCord.getZ() - coordinate.getZ()) * (inputCord.getZ() - coordinate.getZ());

                        double distance = Math.sqrt(xdiff + ydiff +zdiff);
                        if (distance <= radius)
                        {
                            pointList.add(coordinate);
                        }
                    };
                    tupleList.add(new Tuple2<>(inputCord, pointList));
                    return tupleList.iterator();
                }
            });

            cList.saveAsTextFile("file:///home/hadoop/sparkoutput/result_rad_p");
        }
    }

    public void pointDistance()
    {
        SparkConf conf = new SparkConf()
                .setJars(new String []{"/home/hadoop/IdeaProjects/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar"})
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()
                .textFile("/home/hadoop/sparkinput/3d_4.txt", 2)
                .toJavaRDD();

        JavaRDD<Coordinate> coordinates = input.map((Function<String, Coordinate>) s ->
        {
            String[] cord = s.split(" ");
            Coordinate coordinate = new Coordinate(
                    Float.parseFloat(cord[1]),
                    Float.parseFloat(cord[2]),
                    Float.parseFloat(cord[3]));
            return coordinate;
        });

        Coordinate inputCord = coordinates.first();

        JavaRDD<Double> filtered = coordinates
                .mapPartitions(new FlatMapFunction<Iterator<Coordinate>, Double>()
                {
                    @Override
                    public Iterator<Double> call(Iterator<Coordinate> coordinateIterator) throws Exception
                    {
                        List<Double> pointList = new ArrayList<>();
                        while (coordinateIterator.hasNext())
                        {
                            Coordinate coordinate = coordinateIterator.next();
                            float xdiff = (inputCord.getX() - coordinate.getX()) * (inputCord.getX() - coordinate.getX());
                            float ydiff = (inputCord.getY() - coordinate.getY()) * (inputCord.getY() - coordinate.getY());
                            float zdiff = (inputCord.getZ() - coordinate.getZ()) * (inputCord.getZ() - coordinate.getZ());

                            double distance = Math.sqrt(xdiff + ydiff + zdiff);

                            if (distance <= 0.5)
                            {
                                pointList.add(distance);
                            }
                        };

                        return pointList.iterator();
                    }
                });

        List<Double> result =  filtered.collect();

        for (Double d: result) {
            System.out.println(d);
        }

        System.out.println(result.size());
        System.out.println(coordinates.count());
    }

    public void KNN()
    {
        SparkConf conf = new SparkConf()
                .setJars(new String []{"/home/hadoop/IdeaProjects/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar"})
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()
                .textFile("/home/hadoop/sparkinput/3d_4.txt", 2)
                .toJavaRDD();

        JavaRDD<Coordinate> coordinates = input.map((Function<String, Coordinate>) s ->
        {
            String[] cord = s.split(" ");
            Coordinate coordinate = new Coordinate(
                    Float.parseFloat(cord[1]),
                    Float.parseFloat(cord[2]),
                    Float.parseFloat(cord[3]));
            return coordinate;
        });

        JavaRDD<String> sortedCoordinates = coordinates
                .sortBy(new Function<Coordinate, Long>()
                {
                    @Override
                    public Long call(Coordinate v1) throws Exception
                    {
                        return v1.getM();
                    }
                },
                        true,
                        3
                )
                .map(new Function<Coordinate, String>()
                {
                    @Override
                    public String call(Coordinate v1) throws Exception
                    {
                        return  String.valueOf(v1.getM())
                                + ","
                                + String.valueOf(v1.getX())
                                + ","
                                + String.valueOf(v1.getY())
                                + ","
                                + String.valueOf(v1.getZ());
                    }
                });

        // Coordinate p = sortedCoordinates.first();

         /*
         JavaPairRDD<Coordinate, List<Coordinate>>  finalResult = sortedCoordinates
                 .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Coordinate>, Coordinate, List<Coordinate>>()
        {
            long k = 20;
            @Override
            public Iterator<Tuple2<Coordinate, List<Coordinate>>> call(Iterator<Coordinate> coordinateIterator) throws Exception
            {
                long kCountAhead = 0;
                long kCountBack = 0;
                List<Tuple2<Coordinate, List<Coordinate>>> tt = new ArrayList<>();

                List<Coordinate> coordinates = new ArrayList<>();
                while(coordinateIterator.hasNext())
                {
                    Coordinate cord = coordinateIterator.next();
                    if (p.getM() > cord.getM() && kCountAhead < k)
                    {
                        coordinates.add(cord);
                        kCountAhead++;
                    }

                    if (p.getM() < cord.getM() && kCountBack < k)
                    {
                        coordinates.add(cord);
                        kCountBack++;
                    }
                }
                tt.add(new Tuple2<>(p, coordinates));
                return tt.iterator();
            }
        });*/

         sortedCoordinates.saveAsTextFile("file:///home/hadoop/sparkoutput/result");
       /* Map<Coordinate, List<Coordinate>> msps = finalResult.collectAsMap();

        for (Map.Entry<Coordinate, List<Coordinate>> entry : msps.entrySet())
        {
            List<Coordinate> cds = entry.getValue();
            System.out.println(cds.size());

            for (Coordinate cd : cds)
            {
                System.out.println(cd.getM());
            }
        }
        */

        /*
        Dataset<Row> dataset =   spark.createDataFrame(sortedCoordinates, Coordinate.class);
        dataset.createOrReplaceTempView("Coordinate");
        dataset.show(20);
        */
    }
}