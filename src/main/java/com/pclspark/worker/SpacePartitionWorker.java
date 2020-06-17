package com.pclspark.worker;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.pclspark.model.PointCloudRegion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class SpacePartitionWorker
{
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

        JavaRDD<PointCloudRegion> pointCloudsRegions = input
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

                            long pointId = Long.parseLong(cord[0]);
                            long morton = Long.parseLong(cord[1]);
                            long x = Long.parseLong(cord[2]);
                            long y = Long.parseLong(cord[3]);
                            long z = Long.parseLong(cord[4]);
                            float xo = Float.parseFloat(cord[5]);
                            float yo = Float.parseFloat(cord[6]);
                            float zo = Float.parseFloat(cord[7]);
                            int label = Integer.parseInt(cord[9]);

                            if (label == 0)
                                continue;

                            long delta_x = 1000L;

                            if (x <= (95000 + delta_x))
                            {
                                PointCloudRegion pointCloud = new PointCloudRegion();

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

                            if (x >= (95000 - delta_x) &&  x <= (99000 + delta_x))
                            {
                                PointCloudRegion pointCloud = new PointCloudRegion();
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

                            if (x >= (99000 - delta_x) &&  x <= (104000 + delta_x))
                            {
                                PointCloudRegion pointCloud = new PointCloudRegion();

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

                            if (x >= (104000 - delta_x) &&  x <= (108000 + delta_x))
                            {
                                PointCloudRegion pointCloud = new PointCloudRegion();

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

                            if (x >= (108000 - delta_x) &&  x <= (300000 + delta_x))
                            {
                                PointCloudRegion pointCloud = new PointCloudRegion();

                                if (x < 108000 || x > 300000)
                                {
                                    pointCloud.setIsboundary(1);
                                }
                                else
                                {
                                    pointCloud.setIsboundary(0);
                                }

                                pointCloud.setMorton(morton);
                                pointCloud.setRegionid( 5L);
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

        CassandraJavaUtil.javaFunctions(pointCloudsRegions)
                .writerBuilder(
                        "propelld",
                        "pointcloudregionsver",
                        mapToRow(PointCloudRegion.class)
                ).saveToCassandra();
    }
}