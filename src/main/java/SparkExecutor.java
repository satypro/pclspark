import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkExecutor
{
    public static String SPACE_DELIMITER = " ";
    public static void main3(String[] args)
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


       /* SparkConf conf = new SparkConf().setMaster("spark://saty-VirtualBox:7077").setAppName("SparkFileSumApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("/home/hadoop/numbers.txt");
        JavaRDD<String> numberStrings = input.flatMap(s -> Arrays.asList(s.split(SPACE_DELIMITER)).iterator());
        JavaRDD<String> validNumberString = numberStrings.filter(string -> !string.isEmpty());
        JavaRDD<Integer> numbers = validNumberString.map(numberString -> Integer.valueOf(numberString));
        int finalSum = numbers.reduce((x,y) -> x+y);

        System.out.println("Final sum is: " + finalSum);

        sc.close();
        */

        SparkSession spark = SparkSession
                .builder()
                .appName("documentation")
                .master("spark://192.168.29.106:7077")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        List<Row> list=new ArrayList<Row>();
        list.add(RowFactory.create("one"));
        list.add(RowFactory.create("two"));
        list.add(RowFactory.create("three"));
        list.add(RowFactory.create("four"));

        List<org.apache.spark.sql.types.StructField> listOfStructField=
                new ArrayList<org.apache.spark.sql.types.StructField>();
        listOfStructField.add(DataTypes.createStructField("test", DataTypes.StringType, true));
        StructType structType=DataTypes.createStructType(listOfStructField);
        Dataset<Row> data=spark.createDataFrame(list,structType);
        data.show();

        //Lets create the data set of row using the Arrays asList Function
        Dataset<Row> test= spark
                .createDataFrame(Arrays
                        .asList(
                                new Movie("movie1",2323d,"1212"),
                                new Movie("movie2",2323d,"1212"),
                                new Movie("movie3",2323d,"1212"),
                                new Movie("movie4",2323d,"1212")
                        ), Movie.class);

        test.select("name").show();
        test.show();
    }
}