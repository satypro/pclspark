package com.pclspark.worker;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.pclspark.model.Point;
import com.pclspark.model.PointCloud;
import com.pclspark.model.PointCloudRegion;
import com.pclspark.model.PointFeature;
import com.pclspark.morton.Morton64;
import com.pclspark.partitioner.CustomPartitioner;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class SpacePartitionFeatureWorker
{
    public static void Process()
    {
        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "192.168.29.100")
                .setJars(new String[]
                        {
                                "/Users/rpncmac2/Github/pclspark/build/libs/pclspark-all-1.0-SNAPSHOT.jar",
                                "/Users/rpncmac2/.gradle/caches/modules-2/files-2.1/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2/c91029d0882509bedd32877e50e6c2e6528b3e8d/spark-cassandra-connector_2.11-2.4.2.jar",
                                "/Users/rpncmac2/.gradle/caches/modules-2/files-2.1/com.twitter/jsr166e/1.1.0/233098147123ee5ddcd39ffc57ff648be4b7e5b2/jsr166e-1.1.0.jar"
                        })
                .setMaster("spark://192.168.29.110:7077")
                .setAppName("PCL_PROCESSOR_CASSANDRA_SEARCH");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        SparkContext context = spark.sparkContext();

        CassandraTableScanJavaRDD<PointCloudRegion> pointCloudTable = CassandraJavaUtil
                .javaFunctions(context)
                .cassandraTable(
                        "propelld",
                        "pointcloudregions",
                        mapRowTo(PointCloudRegion.class)
                );

        CassandraTableScanJavaRDD<PointCloudRegion> pointCloudRegionRdd = pointCloudTable
                .select("regionid", "morton", "pointid", "x", "y", "z", "xo", "yo", "zo", "label" );


        JavaPairRDD<Long, List<PointCloudRegion>> pairRDD = pointCloudRegionRdd
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<PointCloudRegion>, Long, List<PointCloudRegion>>()
        {
            @Override
            public Iterator<Tuple2<Long, List<PointCloudRegion>>> call(Iterator<PointCloudRegion> pointCloudRegionsIterator) throws Exception
            {
                Map<Long, List<PointCloudRegion>> poinCloudRegionMap = new HashMap<>();
                List<Tuple2<Long, List<PointCloudRegion>>> tuple2s = new ArrayList<>();
                try
                {
                    while (pointCloudRegionsIterator.hasNext())
                    {
                        PointCloudRegion pointCloudRegion = pointCloudRegionsIterator.next();

                        if (poinCloudRegionMap.containsKey(pointCloudRegion.getRegionid()))
                        {
                            poinCloudRegionMap
                                    .get(pointCloudRegion.getRegionid())
                                    .add(pointCloudRegion);
                        }
                        else
                        {
                            List<PointCloudRegion> pointCloudRegions = new ArrayList<>();
                            pointCloudRegions.add(pointCloudRegion);
                            poinCloudRegionMap.put(pointCloudRegion.getRegionid(), pointCloudRegions);
                        }
                    }

                    for (Long key : poinCloudRegionMap.keySet())
                    {
                        System.out.println("KEY :" +  key);
                        tuple2s.add(new Tuple2<Long, List<PointCloudRegion>>(key, poinCloudRegionMap.get(key)));
                    }
                }
                catch (Exception ex)
                {
                    System.out.println("Exception Raised in Code");
                    ex.printStackTrace();
                }

                return tuple2s.iterator();
            }
        });

        // Partition it by RegionId
        pairRDD = pairRDD.partitionBy(new CustomPartitioner(5));

        // Then in Each Partition we will apply our search algorithm
        JavaRDD<PointFeature>  pointFeatureJavaRDD = pairRDD
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<Long, List<PointCloudRegion>>>, PointFeature>()
        {
            @Override
            public Iterator<PointFeature> call(Iterator<Tuple2<Long, List<PointCloudRegion>>> tuple2Iterator) throws Exception
            {
                List<PointFeature> pointFeatures = new ArrayList<>();

                while(tuple2Iterator.hasNext())
                {
                    List<PointCloudRegion> pointCloudRegions = tuple2Iterator.next()._2;

                    //Sort it based on Morton Code
                    Collections.sort(pointCloudRegions, new Comparator<PointCloudRegion>()
                    {
                        @Override
                        public int compare(PointCloudRegion o1, PointCloudRegion o2)
                        {
                            if (o1.getMorton() > o2.getMorton())
                                return 1;

                            if (o1.getMorton() < o2.getMorton())
                                return -1;

                            return 0;
                        }
                    });

                    long radius = 1000L;
                    Morton64 morton64 = new Morton64(2, 32);

                    for (PointCloudRegion pointCloudRegion : pointCloudRegions)
                    {
                        long totalPoints = pointCloudRegions.size();

                        long startIndex_X =  pointCloudRegion.getX() - radius;
                        long startIndex_Y =  pointCloudRegion.getY() - radius;

                        long endIndex_X = pointCloudRegion.getX() + radius;
                        long endIndex_Y = pointCloudRegion.getY() + radius;

                        long startMortonCode = morton64.pack(startIndex_X, startIndex_Y);
                        long endMortonCode = morton64.pack(endIndex_X, endIndex_Y);

                        // Now we need to find the Points within this range
                        // Start and End Morton Code...
                        // We need to Do the Morton Code
                        int index = Collections.binarySearch(pointCloudRegions, pointCloudRegion, new Comparator<PointCloudRegion>()
                        {
                            @Override
                            public int compare(PointCloudRegion o1, PointCloudRegion o2)
                            {
                                if (o1.getMorton() > o2.getMorton())
                                    return 1;
                                if (o1.getMorton() < o2.getMorton())
                                    return -1;
                                return 0;
                            }
                        });

                        int startIndex = index;
                        List<Point> points = new ArrayList<Point>();

                        while(startIndex < totalPoints)
                        {
                            PointCloudRegion rgp = pointCloudRegions.get(startIndex);
                            if (rgp.getMorton() <= endMortonCode)
                            {
                                points.add(new Point(rgp.getXo(),
                                        rgp.getYo(),
                                        rgp.getZo()));
                            }
                            else
                            {
                                break;
                            }

                            startIndex++;
                        }

                        startIndex = index;
                        while(startIndex >= 0)
                        {
                            PointCloudRegion rgp = pointCloudRegions.get(startIndex);
                            if (rgp.getMorton() >= startMortonCode)
                            {
                                points.add(new Point(rgp.getXo(),
                                        rgp.getYo(),
                                        rgp.getZo()));
                            }
                            else
                            {
                                break;
                            }

                            startIndex--;
                        }
                        // Now Save the Points into the Cassandra as the List of Points if we want
                        // Else let us calculate the cs, cl, cp values...

                        double[][] matrix = new double[points.size()][3];

                        int matrixIdx = 0;
                        for(Point neighbour : points)
                        {
                            matrix[matrixIdx][0] = neighbour.getX();
                            matrix[matrixIdx][1] = neighbour.getY();
                            matrix[matrixIdx][2] = neighbour.getZ();
                            matrixIdx++;
                        }

                        RealMatrix cov = new Covariance(MatrixUtils.createRealMatrix(matrix))
                                .getCovarianceMatrix();

                        double[] eigenValues = new EigenDecomposition(cov)
                                .getRealEigenvalues();

                        //Features
                        double cl = (eigenValues[0] - eigenValues[1])/eigenValues[0];
                        double cp = (eigenValues[1] - eigenValues[2])/eigenValues[0];
                        double cs =  eigenValues[2]/eigenValues[0];
                        double omnivariance = Math.cbrt(eigenValues[0]* eigenValues[1] * eigenValues[2]);
                        double anisotropy = (eigenValues[0] - eigenValues[2])/eigenValues[0];
                        double eigenentropy = -1* (eigenValues[0]*Math.log(eigenValues[0])
                                + eigenValues[1]*Math.log(eigenValues[1])
                                + eigenValues[2]*Math.log(eigenValues[2]));
                        double changeOfCurvature = eigenValues[2]/(eigenValues[0] + eigenValues[1] + eigenValues[2]);

                    }
                }

                return pointFeatures.iterator();
            }
        });

        CassandraJavaUtil.javaFunctions(pointFeatureJavaRDD)
                .writerBuilder(
                        "propelld",
                        "pointfeature",
                        mapToRow(PointFeature.class)
                ).saveToCassandra();
        System.out.println("DONE PROCESSING");
    }
}