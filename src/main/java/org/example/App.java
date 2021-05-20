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
        Dataset<Row> df = session.read()
                .option("header", true)
                .csv("data/mnm.csv");
        df.show(5);
    }
}
