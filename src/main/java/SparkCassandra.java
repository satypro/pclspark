import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import com.datastax.spark.connector.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class SparkCassandra
{
    public static void Process()
    {
        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "192.168.29.240")
                .setJars(new String []
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

        JavaRDD<PointCloud> pointClouds = input
                .mapPartitions(new FlatMapFunction<Iterator<String>, PointCloud>()
        {
            @Override
            public Iterator<PointCloud> call(Iterator<String> stringIterator) throws Exception
            {
                List<PointCloud> pointClouds = new ArrayList<>();
                Morton64 m = new Morton64(3, 21);
                while(stringIterator.hasNext())
                {
                    String in = stringIterator.next();
                    String[] cord = in.split(" ");
                    PointCloud pointCloud = new PointCloud();
                    float x = Float.parseFloat(cord[1]);
                    float y = Float.parseFloat(cord[2]);
                    float z = Float.parseFloat(cord[3]);

                    long mCode = m.pack(
                            (int)(x * 1000000),
                            (int)(y * 1000000),
                            (int)(z * 1000000));

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

    public static void PP(JavaRDD<PointCloud> sortedRDD, PointCloud inputPointCloud)
    {
        JavaRDD<PointCloud> candidatePointCloud = sortedRDD
                .mapPartitions(new FlatMapFunction<Iterator<PointCloud>, PointCloud>()
                {
                    @Override
                    public Iterator<PointCloud> call(Iterator<PointCloud> pointCloudIterator) throws Exception
                    {
                        List<PointCloud> pointCloudResult = new ArrayList<PointCloud>();
                        int k = 10;
                        int greaterCount = 0;
                        int lesserCount = 0;

                        while(pointCloudIterator.hasNext())
                        {
                            PointCloud pointCloud = pointCloudIterator.next();
                            if (inputPointCloud.getMortoncode() <= pointCloud.getMortoncode())
                            {
                                if(greaterCount < 11)
                                {
                                    float xdiff = (inputPointCloud.getX() - pointCloud.getX()) * (inputPointCloud.getX() - pointCloud.getX());
                                    float ydiff = (inputPointCloud.getY() - pointCloud.getY()) * (inputPointCloud.getY() - pointCloud.getY());
                                    float zdiff = (inputPointCloud.getZ() - pointCloud.getZ()) * (inputPointCloud.getZ() - pointCloud.getZ());
                                    Long distance = (long)Math.sqrt(xdiff + ydiff + zdiff);
                                    pointCloud.setDistance(distance);
                                    pointCloudResult.add(pointCloud);
                                }
                                greaterCount++;
                            }

                            if (inputPointCloud.getMortoncode() >= pointCloud.getMortoncode())
                            {
                                if (lesserCount < 11)
                                {
                                    float xdiff = (inputPointCloud.getX() - pointCloud.getX()) * (inputPointCloud.getX() - pointCloud.getX());
                                    float ydiff = (inputPointCloud.getY() - pointCloud.getY()) * (inputPointCloud.getY() - pointCloud.getY());
                                    float zdiff = (inputPointCloud.getZ() - pointCloud.getZ()) * (inputPointCloud.getZ() - pointCloud.getZ());
                                    Long distance = (long)Math.sqrt(xdiff + ydiff + zdiff);
                                    pointCloud.setDistance(distance);
                                    pointCloudResult.add(pointCloud);
                                }
                                lesserCount ++;
                            }
                        }
                        return pointCloudResult.iterator();
                    }
                });

        JavaRDD<PointCloud>  finalKNNPoints = candidatePointCloud
                .sortBy(new Function<PointCloud, Long>()
                        {
                            @Override
                            public Long call(PointCloud v1) throws Exception
                            {
                                return v1.getDistance();
                            }
                        },
                        false,
                        1);
       /*
        CassandraJavaUtil.javaFunctions(finalKNNPoints)
                .writerBuilder(
                        "PointCloud_Database",
                        "pointcloudwithoutmortoncode",
                        mapToRow(PointCloud.class)
                ).saveToCassandra();
        */
        System.out.println(inputPointCloud.getPointid());
        /*
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
            System.out.print(point.getX());
            System.out.print(" ");
            System.out.print(point.getY());
            System.out.print(" ");
            System.out.print(point.getZ());
            System.out.println("");
        }
    }

    public static void ProcessRadius()
    {
        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "192.168.29.240")
                .setJars(new String []
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

        CassandraTableScanJavaRDD<PointCloud> pointCloudRdd  = pointCloudTable
                .select("regionid","mortoncode","pointid","x","y","z");

        JavaRDD<PointCloud> sortedRDD = pointCloudRdd
                .sortBy(new Function<PointCloud, Long>()
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
        for (int i = 2990; i< 3010; i++)
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
                .setJars(new String []
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

        CassandraTableScanJavaRDD<PointCloud> pointCloudRdd  = pointCloudTable
                .select("regionid","mortoncode","pointid","x","y","z");

        JavaRDD<PointCloud> sortedRDD = pointCloudRdd
                .sortBy(new Function<PointCloud, Long>()
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
        System.out.println(pp.getPointid());
        for (int i = 2990; i< 3011; i++)
        {
            System.out.println(inputPointCloud.get(i).getPointid());
            System.out.println(inputPointCloud.get(i).getMortoncode());
        }
        /*
        for (PointCloud pointcloudinput : inputPointCloud)
        {
            SparkCassandra.PP(sortedRDD, pointcloudinput);
        }*/

        System.out.println("DONE PROCESSING");
    }
}