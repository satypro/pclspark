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
        SparkCassandra cassandraSpark = new SparkCassandra();
        //cassandraSpark.BuildDataOutputSpaceSplitNonMortonCassandra();
        cassandraSpark.BuildDataOutputSpaceSplitCassandra();
    }
}