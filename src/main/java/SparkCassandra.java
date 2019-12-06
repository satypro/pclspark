import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import com.datastax.spark.connector.*;

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
                                "/home/hadoop/IdeaProjects/pclspark/build/libs/pclspark-1.0-SNAPSHOT.jar",
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

        JavaRDD<PointCloud> pointClouds = input.map((Function<String, PointCloud>) s ->
        {
            String[] cord = s.split(" ");
            PointCloud pointCloud = new PointCloud();
            /*
            Morton3D morton = new Morton3D();
            long mortonCode = morton.encode((int)(Float.parseFloat(cord[1])* 100000),
                    (int)(Float.parseFloat(cord[2])* 100000),
                    (int)(Float.parseFloat(cord[3])* 100000));

            */

            Morton2D morton2D = new Morton2D();
            long mortonCode2d = morton2D.encode(
                    (int)(Float.parseFloat(cord[1]))* 100000,
                       (int)(Float.parseFloat(cord[2])* 100000));

            pointCloud.setRegionid(1);
            pointCloud.setMortoncode(mortonCode2d);
            pointCloud.setPointid(Integer.parseInt(cord[0]));
            pointCloud.setX(Float.parseFloat(cord[1]));
            pointCloud.setY(Float.parseFloat(cord[2]));
            pointCloud.setZ(Float.parseFloat(cord[3]));
            return pointCloud;
        });

        //SparkContext context = new SparkContext(conf);

        /*
            CassandraTableScanJavaRDD<PointCloud> pointCloudTable = CassandraJavaUtil
                    .javaFunctions(context)
                    .cassandraTable(
                            "PointCloud_Database",
                            "pointcloud",
                            mapRowTo(PointCloud.class)
                    );

            CassandraTableScanJavaRDD<PointCloud> pointCloudRdd  = pointCloudTable
                    .select("mortoncode","pointid","x","y","z");

            List<PointCloud> pointCloudsExisting = pointCloudRdd.collect();

            for (PointCloud point : pointCloudsExisting)
            {
                System.out.println(point.getPointid());
            }
         */
        System.out.println(pointClouds.count());
        CassandraJavaUtil.javaFunctions(pointClouds)
                .writerBuilder(
                        "PointCloud_Database",
                           "pointcloudwith2dmortoncode",
                        mapToRow(PointCloud.class)
                ).saveToCassandra();
    }
}
