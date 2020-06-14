package com.pclspark.worker;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.pclspark.model.*;
import com.pclspark.morton.Morton64;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class SparkCassandraObsolete
{
    public static void BuildDataInCassandra()
    {
        SparkConf conf = new SparkConf()
            .set("spark.cassandra.connection.host", "192.168.29.240")
            .setJars(new String[]
                         {
                             "/home/hadoop/IdeaProjects/project/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
                             "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                             "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                         })
            .setMaster("spark://192.168.29.106:7077")
            .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()

            .textFile("/home/hadoop/sparkinput/3d_4.txt", 2)
            .toJavaRDD();
           //3dsemantic.txt
        JavaRDD<PointCloud> pointClouds = input
            .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloud>()
            {
                @Override
                public Iterator<PointCloud> call(Iterator<String> stringIterator) throws Exception
                {
                    double p = 0.0001f;
                    double xMin = 0.984125d;
                    double yMin = 0.0d;
                    double zMin = 0.0159803d;
                    List<PointCloud> pointClouds = new ArrayList<>();
                    Morton64 m = new Morton64(3, 21);
                    while (stringIterator.hasNext())
                    {
                        String in = stringIterator.next();
                        String[] cord = in.split(" ");
                        PointCloud pointCloud = new PointCloud();
                        double x = Double.parseDouble(cord[1]);
                        double y = Double.parseDouble(cord[2]);
                        double z = Double.parseDouble(cord[3]);

                        long xNorm = (long) ((x - xMin)/p);
                        long yNorm = (long) ((y - yMin)/p);
                        long zNorm = (long) ((z - zMin)/p);

                        long mCode = m.pack(
                            xNorm,
                            yNorm,
                            zNorm
                        );

                        pointCloud.setRegionid(1);
                        pointCloud.setMortoncode(mCode);
                        pointCloud.setPointid(Integer.parseInt(cord[0]));
                        pointCloud.setX(Float.parseFloat(cord[1]));
                        pointCloud.setY(Float.parseFloat(cord[2]));
                        pointCloud.setZ(Float.parseFloat(cord[3]));
                        pointClouds.add(pointCloud);
                    }
                    return pointClouds.iterator();
                }
            });

        /*
        JavaRDD<PointCloud> pointClouds = input.map((Function<String, PointCloud>) s ->
        {
            String[] cord = s.split(" ");
            PointCloud pointCloud = new PointCloud();

            Morton3D morton = new Morton3D();
            long mortonCode = morton.encode(
                    (int)(Float.parseFloat(cord[1])* 1000000),
                    (int)(Float.parseFloat(cord[2])* 1000000),
                    (int)(Float.parseFloat(cord[3])* 1000000));

            pointCloud.setRegionid(1);
            pointCloud.setMortoncode(mortonCode);
            pointCloud.setPointid(Integer.parseInt(cord[0]));
            pointCloud.setX(Float.parseFloat(cord[1]));
            pointCloud.setY(Float.parseFloat(cord[2]));
            pointCloud.setZ(Float.parseFloat(cord[3]));
            return pointCloud;
        });
         */

        CassandraJavaUtil.javaFunctions(pointClouds)
            .writerBuilder(
                "PointCloud_Database",
                "pointcloud",
                mapToRow(PointCloud.class)
            ).saveToCassandra();
    }

    public static void BuildDataOutputOCtreeInCassandra()
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
                .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()
                .textFile("/home/research/dataset/octree.txt", 3)
                .toJavaRDD();

        JavaRDD<PointCloudNormalizedOctree> pointClouds = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudNormalizedOctree>()
                {
                    @Override
                    public Iterator<PointCloudNormalizedOctree> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudNormalizedOctree> pointClouds = new ArrayList<>();
                        Morton64 m = new Morton64(3, 21);
                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudNormalizedOctree pointCloud = new PointCloudNormalizedOctree();

                            long pointId = Long.parseLong(cord[0]);
                            long mCode = Long.parseLong(cord[1]);
                            long x = Long.parseLong(cord[2]);
                            long y = Long.parseLong(cord[3]);
                            long z = Long.parseLong(cord[4]);
                            float xo = Float.parseFloat(cord[5]);
                            float yo = Float.parseFloat(cord[6]);
                            float zo = Float.parseFloat(cord[7]);
                            String regionId = cord[8];


                            pointCloud.setMortoncode(mCode);
                            pointCloud.setX(x);
                            pointCloud.setY(y);pointCloud.setPointid(pointId);
                            pointCloud.setZ(z);
                            pointCloud.setXo(xo);
                            pointCloud.setYo(yo);
                            pointCloud.setZo(zo);
                            pointCloud.setRegionid(regionId);

                            pointClouds.add(pointCloud);
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointClouds)
                .writerBuilder(
                        "propelld",
                        "pointcloudnormalizedoctree",
                        mapToRow(PointCloudNormalizedOctree.class)
                ).saveToCassandra();
    }

    public static void BuildDataOutputSpaceSplitCassandra()
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
                .textFile("/home/research/dataset/octree.txt", 3)
                .toJavaRDD();

        JavaRDD<PointCloudRegion> pointCloudsRegion1 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudRegion>()
                {
                    @Override
                    public Iterator<PointCloudRegion> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudRegion> pointClouds = new ArrayList<>();
                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudRegion pointCloud = new PointCloudRegion();

                            long x = Long.parseLong(cord[2]);
                            long delta_x = 1000L;
                            if (x <= (95000 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;
                                long morton = Long.parseLong(cord[1]);
                                long y = Long.parseLong(cord[3]);
                                long z = Long.parseLong(cord[4]);
                                float xo = Float.parseFloat(cord[5]);
                                float yo = Float.parseFloat(cord[6]);
                                float zo = Float.parseFloat(cord[7]);
                                int label = Integer.parseInt(cord[9]);

                                if (x > 95000)
                                {
                                    pointCloud.setIsboundary(1);
                                }
                                else
                                {
                                    pointCloud.setIsboundary(0);
                                }

                                pointCloud.setMorton(morton);
                                pointCloud.setRegionid( 1L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);
                                pointCloud.setXo(xo);
                                pointCloud.setYo(yo);
                                pointCloud.setZo(zo);
                                pointCloud.setLabel(label);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion1)
                .writerBuilder(
                        "propelld",
                        "pointcloudregions",
                        mapToRow(PointCloudRegion.class)
                ).saveToCassandra();


        JavaRDD<PointCloudRegion> pointCloudsRegion2 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudRegion>()
                {
                    @Override
                    public Iterator<PointCloudRegion> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudRegion> pointClouds = new ArrayList<>();

                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudRegion pointCloud = new PointCloudRegion();

                            long x = Long.parseLong(cord[2]);
                            long delta_x = 1000L;
                            if (x >= (95000 - delta_x) &&  x <= (99000 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;
                                long y = Long.parseLong(cord[3]);
                                long z = Long.parseLong(cord[4]);
                                float xo = Float.parseFloat(cord[5]);
                                float yo = Float.parseFloat(cord[6]);
                                float zo = Float.parseFloat(cord[7]);
                                long morton = Long.parseLong(cord[1]);
                                int label = Integer.parseInt(cord[9]);

                                if (x < 95000 || x > 99000)
                                {
                                    pointCloud.setIsboundary(1);
                                }
                                else
                                {
                                    pointCloud.setIsboundary(0);
                                }

                                pointCloud.setMorton(morton);
                                pointCloud.setRegionid( 2L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);
                                pointCloud.setXo(xo);
                                pointCloud.setYo(yo);
                                pointCloud.setZo(zo);
                                pointCloud.setLabel(label);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion2)
                .writerBuilder(
                        "propelld",
                        "pointcloudregions",
                        mapToRow(PointCloudRegion.class)
                ).saveToCassandra();

        JavaRDD<PointCloudRegion> pointCloudsRegion3 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudRegion>()
                {
                    @Override
                    public Iterator<PointCloudRegion> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudRegion> pointClouds = new ArrayList<>();

                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudRegion pointCloud = new PointCloudRegion();

                            long x = Long.parseLong(cord[2]);
                            long delta_x = 1000L;
                            if (x >= (99000 - delta_x) &&  x <= (104000 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;
                                long y = Long.parseLong(cord[3]);
                                long z = Long.parseLong(cord[4]);
                                float xo = Float.parseFloat(cord[5]);
                                float yo = Float.parseFloat(cord[6]);
                                float zo = Float.parseFloat(cord[7]);
                                long morton = Long.parseLong(cord[1]);
                                int label = Integer.parseInt(cord[9]);

                                if (x < 99000 || x > 104000)
                                {
                                    pointCloud.setIsboundary(1);
                                }
                                else
                                {
                                    pointCloud.setIsboundary(0);
                                }

                                pointCloud.setMorton(morton);
                                pointCloud.setRegionid( 3L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);
                                pointCloud.setXo(xo);
                                pointCloud.setYo(yo);
                                pointCloud.setZo(zo);
                                pointCloud.setLabel(label);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion3)
                .writerBuilder(
                        "propelld",
                        "pointcloudregions",
                        mapToRow(PointCloudRegion.class)
                ).saveToCassandra();

        JavaRDD<PointCloudRegion> pointCloudsRegion4 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudRegion>()
                {
                    @Override
                    public Iterator<PointCloudRegion> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudRegion> pointClouds = new ArrayList<>();

                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudRegion pointCloud = new PointCloudRegion();

                            long x = Long.parseLong(cord[2]);
                            long delta_x = 1000L;
                            if (x >= (104000 - delta_x) &&  x <= (108000 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;
                                long y = Long.parseLong(cord[3]);
                                long z = Long.parseLong(cord[4]);
                                float xo = Float.parseFloat(cord[5]);
                                float yo = Float.parseFloat(cord[6]);
                                float zo = Float.parseFloat(cord[7]);
                                long morton = Long.parseLong(cord[1]);
                                int label = Integer.parseInt(cord[9]);

                                if (x < 104000 || x > 108000)
                                {
                                    pointCloud.setIsboundary(1);
                                }
                                else
                                {
                                    pointCloud.setIsboundary(0);
                                }

                                pointCloud.setMorton(morton);
                                pointCloud.setRegionid( 4L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);
                                pointCloud.setXo(xo);
                                pointCloud.setYo(yo);
                                pointCloud.setZo(zo);
                                pointCloud.setLabel(label);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion4)
                .writerBuilder(
                        "propelld",
                        "pointcloudregions",
                        mapToRow(PointCloudRegion.class)
                ).saveToCassandra();

        JavaRDD<PointCloudRegion> pointCloudsRegion5 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudRegion>()
                {
                    @Override
                    public Iterator<PointCloudRegion> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudRegion> pointClouds = new ArrayList<>();

                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudRegion pointCloud = new PointCloudRegion();

                            long x = Long.parseLong(cord[2]);
                            long delta_x = 1000L;
                            if (x >= (108000 - delta_x) &&  x <= (300000 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;
                                long y = Long.parseLong(cord[3]);
                                long z = Long.parseLong(cord[4]);
                                float xo = Float.parseFloat(cord[5]);
                                float yo = Float.parseFloat(cord[6]);
                                float zo = Float.parseFloat(cord[7]);
                                long morton = Long.parseLong(cord[1]);
                                int label = Integer.parseInt(cord[9]);

                                if (x < 108000 || x > 300000)
                                {
                                    pointCloud.setIsboundary(1);
                                }
                                else
                                {
                                    pointCloud.setIsboundary(0);
                                }

                                pointCloud.setMorton(morton);
                                pointCloud.setRegionid( 4L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);
                                pointCloud.setXo(xo);
                                pointCloud.setYo(yo);
                                pointCloud.setZo(zo);
                                pointCloud.setLabel(label);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion5)
                .writerBuilder(
                        "propelld",
                        "pointcloudregions",
                        mapToRow(PointCloudRegion.class)
                ).saveToCassandra();
    }

    public static void BuildDataOutputSpaceSplitNonMortonCassandra()
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
                .textFile("/home/research/dataset/octree.txt", 3)
                .toJavaRDD();

        JavaRDD<PointCloudPartition> pointCloudsRegion1 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudPartition>()
                {
                    @Override
                    public Iterator<PointCloudPartition> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudPartition> pointClouds = new ArrayList<>();
                        Morton64 m = new Morton64(3, 21);
                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudPartition pointCloud = new PointCloudPartition();

                            float xn = Float.parseFloat(cord[1]);
                            float delta_x = 0.03f;
                            if (xn <= (-0.05005642 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;

                                float yn = Float.parseFloat(cord[2]);
                                float zn = Float.parseFloat(cord[3]);

                                float x = Float.parseFloat(cord[4]);
                                float y = Float.parseFloat(cord[5]);
                                float z = Float.parseFloat(cord[6]);

                                pointCloud.setRegionid(1L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setXn(xn);
                                pointCloud.setYn(yn);
                                pointCloud.setZn(zn);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion1)
                .writerBuilder(
                        "propelld",
                        "pointpartitionregions",
                        mapToRow(PointCloudPartition.class)
                ).saveToCassandra();

        JavaRDD<PointCloudPartition> pointCloudsRegion2 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudPartition>()
                {
                    @Override
                    public Iterator<PointCloudPartition> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudPartition> pointClouds = new ArrayList<>();
                        Morton64 m = new Morton64(3, 21);
                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudPartition pointCloud = new PointCloudPartition();

                            float xn = Float.parseFloat(cord[1]);
                            float delta_x = 0.03f;
                            if (xn >= (0 - delta_x) &&  xn <= (0.05005642 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;

                                float yn = Float.parseFloat(cord[2]);
                                float zn = Float.parseFloat(cord[3]);

                                float x = Float.parseFloat(cord[4]);
                                float y = Float.parseFloat(cord[5]);
                                float z = Float.parseFloat(cord[6]);

                                pointCloud.setRegionid(2L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setXn(xn);
                                pointCloud.setYn(yn);
                                pointCloud.setZn(zn);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion2)
                .writerBuilder(
                        "propelld",
                        "pointpartitionregions",
                        mapToRow(PointCloudPartition.class)
                ).saveToCassandra();

        JavaRDD<PointCloudPartition> pointCloudsRegion3 = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudPartition>()
                {
                    @Override
                    public Iterator<PointCloudPartition> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudPartition> pointClouds = new ArrayList<>();
                        Morton64 m = new Morton64(3, 21);
                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudPartition pointCloud = new PointCloudPartition();

                            float xn = Float.parseFloat(cord[1]);
                            float delta_x = 0.03f;
                            if (xn >= (0.05005642 - delta_x) &&  xn <= (0.9143333 + delta_x))
                            {
                                long pointId = Long.parseLong(cord[0]);;

                                float yn = Float.parseFloat(cord[2]);
                                float zn = Float.parseFloat(cord[3]);

                                float x = Float.parseFloat(cord[4]);
                                float y = Float.parseFloat(cord[5]);
                                float z = Float.parseFloat(cord[6]);

                                pointCloud.setRegionid(3L);
                                pointCloud.setPointid(pointId);
                                pointCloud.setXn(xn);
                                pointCloud.setYn(yn);
                                pointCloud.setZn(zn);
                                pointCloud.setX(x);
                                pointCloud.setY(y);
                                pointCloud.setZ(z);

                                pointClouds.add(pointCloud);
                            }
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointCloudsRegion3)
                .writerBuilder(
                        "propelld",
                        "pointpartitionregions",
                        mapToRow(PointCloudPartition.class)
                ).saveToCassandra();
    }

    public static void BuildDataOutputInCassandra()
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
                .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()
                .textFile("/home/research/dataset/3dsemantic.txt", 2)
                .toJavaRDD();

        JavaRDD<PointCloudNormalized> pointClouds = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudNormalized>()
                {
                    @Override
                    public Iterator<PointCloudNormalized> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudNormalized> pointClouds = new ArrayList<>();
                        Morton64 m = new Morton64(3, 21);
                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudNormalized pointCloud = new PointCloudNormalized();
                            long x = Long.parseLong(cord[1]);
                            long y = Long.parseLong(cord[2]);
                            long z = Long.parseLong(cord[3]);

                            long mCode = m.pack(x, y, z);

                            pointCloud.setRegionid(1);
                            pointCloud.setMortoncode(mCode);
                            pointCloud.setPointid(Long.parseLong(cord[0]));
                            pointCloud.setX(x);
                            pointCloud.setY(y);
                            pointCloud.setZ(z);
                            pointClouds.add(pointCloud);
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointClouds)
                .writerBuilder(
                        "propelld",
                        "pointcloudnormalized",
                        mapToRow(PointCloudNormalized.class)
                ).saveToCassandra();
    }

    public static void BuildDataOutputVehingenInCassandra()
    {
        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "192.168.29.240")
                .setJars(new String[]
                        {
                                "/home/hadoop/IdeaProjects/project/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
                                "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                                "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                        })
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> input = spark.sparkContext()

                .textFile("/home/hadoop/sparkinput/vhdata.txt", 2)
                .toJavaRDD();

        JavaRDD<PointCloudNormalized> pointClouds = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloudNormalized>()
                {
                    @Override
                    public Iterator<PointCloudNormalized> call(Iterator<String> stringIterator) throws Exception
                    {
                        List<PointCloudNormalized> pointClouds = new ArrayList<>();
                        Morton64 m = new Morton64(3, 21);
                        while (stringIterator.hasNext())
                        {
                            String in = stringIterator.next();
                            String[] cord = in.split(" ");
                            PointCloudNormalized pointCloud = new PointCloudNormalized();
                            long x = Long.parseLong(cord[1]);
                            long y = Long.parseLong(cord[2]);
                            long z = Long.parseLong(cord[3]);

                            long mCode = m.pack(x, y, z);

                            pointCloud.setRegionid(1);
                            pointCloud.setMortoncode(mCode);
                            pointCloud.setPointid(Long.parseLong(cord[0]));
                            pointCloud.setX(x);
                            pointCloud.setY(y);
                            pointCloud.setZ(z);
                            pointClouds.add(pointCloud);
                        }
                        return pointClouds.iterator();
                    }
                });

        CassandraJavaUtil.javaFunctions(pointClouds)
                .writerBuilder(
                        "PointCloud_Database",
                        "pointcloudnormalizedvehingen",
                        mapToRow(PointCloudNormalized.class)
                ).saveToCassandra();
    }

    public static void RadiusSearchNormalized(JavaRDD<PointCloudNormalized> sortedRDD, PointCloudNormalized inputPointCloud, long radius)
    {
        Morton64 m = new Morton64(3, 21);
        long mCodeUpper = m.pack(inputPointCloud.getX() + radius, inputPointCloud.getY() + radius, inputPointCloud.getZ() + radius);
        long x = inputPointCloud.getX() - radius < 0 ? 0 : inputPointCloud.getX() - radius;
        long y = inputPointCloud.getY() - radius < 0 ? 0 : inputPointCloud.getY() - radius;
        long z = inputPointCloud.getZ() - radius < 0 ? 0 : inputPointCloud.getZ() - radius;
        long mCodeLower = m.pack( x, y, z);
        JavaRDD<PointCloudNormalized> candidatePointCloud = sortedRDD
                .mapPartitions(new FlatMapFunction<Iterator<PointCloudNormalized>, PointCloudNormalized>()
                {
                    @Override
                    public Iterator<PointCloudNormalized> call(Iterator<PointCloudNormalized> pointCloudIterator) throws Exception
                    {
                        List<PointCloudNormalized> pointCloudResult = new ArrayList<PointCloudNormalized>();
                        while (pointCloudIterator.hasNext())
                        {
                            PointCloudNormalized pointCloud = pointCloudIterator.next();
                            float xdiff = (inputPointCloud.getX() - pointCloud.getX()) * (inputPointCloud
                                    .getX() - pointCloud.getX());
                            float ydiff = (inputPointCloud.getY() - pointCloud.getY()) * (inputPointCloud
                                    .getY() - pointCloud.getY());
                            float zdiff = (inputPointCloud.getZ() - pointCloud.getZ()) * (inputPointCloud
                                    .getZ() - pointCloud.getZ());
                            double distance = xdiff + ydiff + zdiff;
                            pointCloud.setDistance(distance);

                            if (pointCloud.getMortoncode()  >= mCodeLower
                                    && pointCloud.getMortoncode() <= mCodeUpper && (Math.sqrt(distance) <= radius))
                            {
                                pointCloudResult.add(pointCloud);
                            }
                        }
                        return pointCloudResult.iterator();
                    }
                });

        List<PointCloudNormalized> pointCloudsExisting = candidatePointCloud.collect();
        for (PointCloudNormalized point : pointCloudsExisting)
        {
            System.out.print(point.getPointid());
            System.out.print(" ");
            System.out.print(point.getDistance());
            System.out.print(" ");
            System.out.print(point.getX());
            System.out.print(" ");
            System.out.print(point.getY());
            System.out.print(" ");
            System.out.print(point.getZ());
            System.out.print(" ");
            System.out.println(point.getMortoncode());
        }
        System.out.println("DONE Radius Search");
    }

    public static void KNNSearchNormalized(JavaRDD<PointCloudNormalized> sortedRDD, PointCloudNormalized inputPointCloud)
    {
        JavaRDD<PointCloudNormalized> candidatePointCloud = sortedRDD
                .mapPartitions(new FlatMapFunction<Iterator<PointCloudNormalized>, PointCloudNormalized>()
                {
                    @Override
                    public Iterator<PointCloudNormalized> call(Iterator<PointCloudNormalized> pointCloudIterator) throws Exception
                    {
                        List<PointCloudNormalized> pointCloudResult = new ArrayList<PointCloudNormalized>();
                        int a = 10; // accuracy factor
                        int k = 15 * 10;
                        int firstKCount = 1;
                        int windowKCount = 1;
                        int windowStartIndex = 0;
                        while (pointCloudIterator.hasNext())
                        {
                            PointCloudNormalized pointCloud = pointCloudIterator.next();
                            float xdiff = (inputPointCloud.getX() - pointCloud.getX()) * (inputPointCloud
                                    .getX() - pointCloud.getX());
                            float ydiff = (inputPointCloud.getY() - pointCloud.getY()) * (inputPointCloud
                                    .getY() - pointCloud.getY());
                            float zdiff = (inputPointCloud.getZ() - pointCloud.getZ()) * (inputPointCloud
                                    .getZ() - pointCloud.getZ());
                            double distance = xdiff + ydiff + zdiff;
                            pointCloud.setDistance(distance);

                            // Left Side Partition
                            if (inputPointCloud.getMortoncode() > pointCloud.getMortoncode())
                            {
                                if (firstKCount <= k)
                                {
                                    pointCloudResult.add(pointCloud);
                                    firstKCount++;
                                }

                                if (firstKCount > k)
                                {
                                    if (windowStartIndex == k)
                                    {
                                        windowStartIndex = 0;
                                    }
                                    pointCloudResult.set(windowStartIndex, pointCloud);
                                    windowStartIndex++;
                                }
                            }

                            //Right Side Partitions
                            if (inputPointCloud.getMortoncode() <= pointCloud.getMortoncode())
                            {
                                if (windowKCount <= k)
                                {
                                    pointCloudResult.add(pointCloud);
                                    windowKCount++;
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                        return pointCloudResult.iterator();
                    }
                });

        JavaRDD<PointCloudNormalized> finalKNNPoints = candidatePointCloud
                .sortBy(
                        new Function<PointCloudNormalized, Double>()
                        {
                            @Override
                            public Double call(PointCloudNormalized v1) throws Exception
                            {
                                return v1.getDistance();
                            }
                        },
                        true,
                        3
                );

       /*
        CassandraJavaUtil.javaFunctions(finalKNNPoints)
                .writerBuilder(
                        "PointCloud_Database",
                        "pointcloudwithoutmortoncode",
                        mapToRow(PointCloud.class)
                ).saveToCassandra();
        */

        /*
        System.out.println(inputPointCloud.getPointid());

        System.out.println(inputPointCloud.getX());
        System.out.println(inputPointCloud.getY());
        System.out.println(inputPointCloud.getZ());
        System.out.println("*********************");
        */

        //List<PointCloudNormalized> pointCloudsExisting = candidatePointCloud.collect();
        List<PointCloudNormalized> pointCloudsExisting = finalKNNPoints.collect();
        for (PointCloudNormalized point : pointCloudsExisting)
        {
            System.out.print(point.getPointid());
            System.out.print(" ");
            System.out.print(point.getDistance());
            System.out.print(" ");
            System.out.print(point.getX());
            System.out.print(" ");
            System.out.print(point.getY());
            System.out.print(" ");
            System.out.print(point.getZ());
            System.out.print(" ");
            System.out.println(point.getMortoncode());

            /*
            System.out.print(" ");
            System.out.print(point.getX());
            System.out.print(" ");
            System.out.print(point.getY());
            System.out.print(" ");
            System.out.print(point.getZ());
            System.out.println("");
            */
        }
        System.out.println("DONE FIRST");
    }

    public static void KNNSearch(JavaRDD<PointCloud> sortedRDD, PointCloud inputPointCloud)
    {
        JavaRDD<PointCloud> candidatePointCloud = sortedRDD
            .mapPartitions(new FlatMapFunction<Iterator<PointCloud>, PointCloud>()
            {
                @Override
                public Iterator<PointCloud> call(Iterator<PointCloud> pointCloudIterator) throws Exception
                {
                    List<PointCloud> pointCloudResult = new ArrayList<PointCloud>();

                    int k = 10;
                    int firstKCount = 1;
                    int windowKCount = 1;
                    int windowStartIndex = 0;
                    while (pointCloudIterator.hasNext())
                    {
                        PointCloud pointCloud = pointCloudIterator.next();
                        float xdiff = (inputPointCloud.getX() - pointCloud.getX()) * (inputPointCloud
                            .getX() - pointCloud.getX());
                        float ydiff = (inputPointCloud.getY() - pointCloud.getY()) * (inputPointCloud
                            .getY() - pointCloud.getY());
                        float zdiff = (inputPointCloud.getZ() - pointCloud.getZ()) * (inputPointCloud
                            .getZ() - pointCloud.getZ());
                        double distance = Math.sqrt(xdiff + ydiff + zdiff);
                        pointCloud.setDistance(distance);

                        // Left Side Partition
                        if (inputPointCloud.getMortoncode() > pointCloud.getMortoncode())
                        {
                            if (firstKCount <= k)
                            {
                                pointCloudResult.add(pointCloud);
                                firstKCount++;
                            }

                            if (firstKCount > k)
                            {
                                if (windowStartIndex == k)
                                {
                                    windowStartIndex = 0;
                                }
                                pointCloudResult.set(windowStartIndex, pointCloud);
                                windowStartIndex++;
                            }
                        }

                        //Right Side Partitions
                        if (inputPointCloud.getMortoncode() <= pointCloud.getMortoncode())
                        {
                            if (windowKCount <= k)
                            {
                                pointCloudResult.add(pointCloud);
                                windowKCount++;
                            }
                        }
                    }
                    return pointCloudResult.iterator();
                }
            });

        JavaRDD<PointCloud> finalKNNPoints = candidatePointCloud
            .sortBy(
                new Function<PointCloud, Double>()
                {
                    @Override
                    public Double call(PointCloud v1) throws Exception
                    {
                        return v1.getDistance();
                    }
                },
                true,
                1
            );
       /*
        CassandraJavaUtil.javaFunctions(finalKNNPoints)
                .writerBuilder(
                        "PointCloud_Database",
                        "pointcloudwithoutmortoncode",
                        mapToRow(PointCloud.class)
                ).saveToCassandra();
        */

        /*
        System.out.println(inputPointCloud.getPointid());

        System.out.println(inputPointCloud.getX());
        System.out.println(inputPointCloud.getY());
        System.out.println(inputPointCloud.getZ());
        System.out.println("*********************");
        */

        List<PointCloud> pointCloudsExisting = finalKNNPoints.collect();
        for (PointCloud point : pointCloudsExisting)
        {
            System.out.print(point.getPointid());
            System.out.print(" ");
            System.out.println(point.getDistance());
            /*
            System.out.print(" ");
            System.out.print(point.getX());
            System.out.print(" ");
            System.out.print(point.getY());
            System.out.print(" ");
            System.out.print(point.getZ());
            System.out.println("");
            */
        }
        System.out.println("DONE FIRST");
    }

    public static void ProcessRadius()
    {
        SparkConf conf = new SparkConf()
            .set("spark.cassandra.connection.host", "192.168.29.240")
            .setJars(new String[]
                         {
                             "/home/hadoop/IdeaProjects/project/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
                             "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                             "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                         })
            .setMaster("spark://192.168.29.106:7077")
            .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();

        SparkContext context = spark.sparkContext();

        CassandraTableScanJavaRDD<PointCloud> pointCloudTable = CassandraJavaUtil
            .javaFunctions(context)
            .cassandraTable(
                "PointCloud_Database",
                "pointcloud",
                mapRowTo(PointCloud.class)
            );

        CassandraTableScanJavaRDD<PointCloud> pointCloudRdd = pointCloudTable
            .select("regionid", "mortoncode", "pointid", "x", "y", "z");

        JavaRDD<PointCloud> sortedRDD = pointCloudRdd
            .sortBy(
                new Function<PointCloud, Long>()
                {
                    @Override
                    public Long call(PointCloud pointCloud) throws Exception
                    {
                        return pointCloud.getMortoncode();
                    }
                },
                true,
                3
            );

        List<PointCloud> inputPointCloud = sortedRDD.collect();
        sortedRDD.persist(StorageLevel.MEMORY_AND_DISK());

        /*
        sortedRDD.foreach(new VoidFunction<PointCloud>()
        {
            @Override
            public void call(PointCloud pointCloud) throws Exception
            {
                SparkCassandra.PP(sortedRDD, pointCloud);
            }
        });
         */

        PointCloud pp = inputPointCloud.get(3000);
        for (int i = 2990; i < 3010; i++)
        {
            System.out.println(inputPointCloud.get(i));
        }

        //SparkCassandra.PP(sortedRDD, pp);
        /*
        for (PointCloud pointcloudinput : inputPointCloud)
        {
            SparkCassandra.PP(sortedRDD, pointcloudinput);
        }*/

        System.out.println("DONE PROCESSING");
    }

    public static void ProcessKNN()
    {
        SparkConf conf = new SparkConf()
            .set("spark.cassandra.connection.host", "192.168.29.240")
            .setJars(new String[]
                         {
                             "/home/hadoop/IdeaProjects/project/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
                             "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                             "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                         })
            .setMaster("spark://192.168.29.106:7077")
            .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();

        SparkContext context = spark.sparkContext();

        CassandraTableScanJavaRDD<PointCloud> pointCloudTable = CassandraJavaUtil
            .javaFunctions(context)
            .cassandraTable(
                "PointCloud_Database",
                "pointcloud",
                mapRowTo(PointCloud.class)
            );

        CassandraTableScanJavaRDD<PointCloud> pointCloudRdd = pointCloudTable
            .select("regionid", "mortoncode", "pointid", "x", "y", "z");

        JavaRDD<PointCloud> sortedRDD = pointCloudRdd
            .sortBy(
                new Function<PointCloud, Long>()
                {
                    @Override
                    public Long call(PointCloud pointCloud) throws Exception
                    {
                        return pointCloud.getMortoncode();
                    }
                },
                true,
                2
            );

        sortedRDD.persist(StorageLevel.MEMORY_AND_DISK());

        List<PointCloud> inputPointCloud = pointCloudRdd.collect();
        PointCloud inputPoint = inputPointCloud.get(5000);
        System.out.println(inputPoint.getPointid());
        SparkCassandraObsolete.KNNSearch(sortedRDD, inputPoint);

        System.out.println("DONE PROCESSING");
    }

    public static void ProcessKNNNormalized()
    {
        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "192.168.29.240")
                .setJars(new String[]
                        {
                                "/home/hadoop/IdeaProjects/project/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
                                "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                                "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                        })
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        SparkContext context = spark.sparkContext();

        CassandraTableScanJavaRDD<PointCloudNormalized> pointCloudTable = CassandraJavaUtil
                .javaFunctions(context)
                .cassandraTable(
                        "PointCloud_Database",
                        "pointcloudnormalized",
                        mapRowTo(PointCloudNormalized.class)
                );

        CassandraTableScanJavaRDD<PointCloudNormalized> pointCloudRdd = pointCloudTable
                .select("regionid", "mortoncode", "pointid", "x", "y", "z");

        JavaRDD<PointCloudNormalized> sortedRDD = pointCloudRdd
                .sortBy(
                        new Function<PointCloudNormalized, Long>()
                        {
                            @Override
                            public Long call(PointCloudNormalized pointCloud) throws Exception
                            {
                                return pointCloud.getMortoncode();
                            }
                        },
                        true,
                        3
                );

        /*
        JavaPairRDD<Long, PointCloudNormalized> pairRDD = sortedRDD.mapToPair(new PairFunction<PointCloudNormalized, Long, PointCloudNormalized>()
        {
            @Override
            public Tuple2<Long, PointCloudNormalized> call(PointCloudNormalized pointCloudNormalized) throws Exception
            {
                return new Tuple2<>(pointCloudNormalized.getMortoncode(), pointCloudNormalized);
            }
        });
        */
       // pairRDD.partitionBy(new RangePartitioner(4, pairRDD,true))

        sortedRDD.persist(StorageLevel.MEMORY_AND_DISK());

        PointCloudNormalized inputPoint = new PointCloudNormalized();
        inputPoint.setRegionid(1);
        inputPoint.setPointid(11861481L);
        inputPoint.setMortoncode(670208810875044L);
        inputPoint.setX(41008L);
        inputPoint.setY(108884L);
        inputPoint.setZ(3931L);
        inputPoint.setDistance(0);

        System.out.println(inputPoint.getPointid());
        SparkCassandraObsolete.KNNSearchNormalized(sortedRDD, inputPoint);
        System.out.println("DONE PROCESSING1");

        inputPoint = new PointCloudNormalized();
        inputPoint.setRegionid(1);
        inputPoint.setPointid(6008243L);
        inputPoint.setMortoncode(612715888602887L);
        inputPoint.setX(51849L);
        inputPoint.setY(90297L);
        inputPoint.setZ(5309L);
        inputPoint.setDistance(0);

        inputPoint = new PointCloudNormalized();
        inputPoint.setRegionid(1);
        inputPoint.setPointid(3298773L);
        inputPoint.setMortoncode(607728958050516L);
        inputPoint.setX(46740L);
        inputPoint.setY(87110L);
        inputPoint.setZ(4001L);
        inputPoint.setDistance(0);

        System.out.println(inputPoint.getPointid());
        SparkCassandraObsolete.KNNSearchNormalized(sortedRDD, inputPoint);
        System.out.println("DONE PROCESSING2");
    }

    public static void ProcessKNNNormalizedvh()
    {
        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "192.168.29.240")
                .setJars(new String[]
                        {
                                "/home/hadoop/IdeaProjects/project/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
                                "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                                "/home/hadoop/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                        })
                .setMaster("spark://192.168.29.106:7077")
                .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        SparkContext context = spark.sparkContext();

        CassandraTableScanJavaRDD<PointCloudNormalized> pointCloudTable = CassandraJavaUtil
                .javaFunctions(context)
                .cassandraTable(
                        "PointCloud_Database",
                        "pointcloudnormalizedvehingen",
                        mapRowTo(PointCloudNormalized.class)
                );

        CassandraTableScanJavaRDD<PointCloudNormalized> pointCloudRdd = pointCloudTable
                .select("regionid", "mortoncode", "pointid", "x", "y", "z");

        JavaRDD<PointCloudNormalized> sortedRDD = pointCloudRdd
                .sortBy(
                        new Function<PointCloudNormalized, Long>()
                        {
                            @Override
                            public Long call(PointCloudNormalized pointCloud) throws Exception
                            {
                                return pointCloud.getMortoncode();
                            }
                        },
                        true,
                        3
                );

        sortedRDD.persist(StorageLevel.MEMORY_AND_DISK());

        PointCloudNormalized inputPoint = new PointCloudNormalized();
        inputPoint.setRegionid(1);
        inputPoint.setPointid(206921L);
        inputPoint.setMortoncode(307133882454940664L);
        inputPoint.setX(371454L);
        inputPoint.setY(611590L);
        inputPoint.setZ(1134L);
        inputPoint.setDistance(0);

        System.out.println(inputPoint.getPointid());
        SparkCassandraObsolete.RadiusSearchNormalized(sortedRDD, inputPoint, 10000);
        System.out.println("DONE PROCESSING");
    }
}