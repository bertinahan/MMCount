package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        start();
        System.out.println( "Hello World!" );
    }

    public static void start()
    {

        SparkSession session = SparkSession.builder()
                .appName("MM_Count")
                .master("local")
                .getOrCreate();
        StructType schema = new StructType(new StructField[]
                {
                        new StructField("State", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("Color", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("Count", DataTypes.IntegerType, true, Metadata.empty()),
                });
        Dataset<Row> df = session.read()
                .option("header", true)
                .schema(schema)
                .csv("data/mnm.csv");
        Map map = new HashMap<String, String>();
        map.put("Count", "count");
        df.select("State", "Color", "Count")
                .groupBy("State", "Color")
                .agg(map)
                .show(false);
    }
}
