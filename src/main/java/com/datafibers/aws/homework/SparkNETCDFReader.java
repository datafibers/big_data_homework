package com.datafibers.aws.homework;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.Dataset;

import java.io.Serializable;
import java.util.*;

/**
 * SparkNETCDFReader
 * Read one NetCDF for each downloaded from AWS S3 and dump to database
 */
public class SparkNETCDFReader implements Serializable {

    /**
     * Read list of NetCDF in one spark job. Extract data and dump to the databaes.
     * @param sparkMaster running mode
     * @param appName application name
     * @param fileParameters parameters for the list of files to read
     * @param dbProperties database property
     */
    public void readListFilesAndDump(String sparkMaster, String appName, List<String> fileParameters, Properties dbProperties) {

        SparkConf sparkConf = new SparkConf().setMaster(sparkMaster).setAppName(appName + " - 1. NetCDF processing");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        /*
         Read one NetCDF to a list of String rdd from file input parameters.
         Then flat it to String rdd
         The file parameter syntax is "file_name,measure,coordy,coordx,limit".
          */
        JavaRDD<String> msgRDD = javaSparkContext //
                .parallelize(fileParameters)
                .map(new SparkNETCDFMapPartition())
                .flatMap(new FlatMapFunction<List<String>, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterator<String> call(List<String> list) {
                        return list.iterator();
                    }
                });

        /**
         * Define a schema and convert Java rdd to Dataset rdd
         * The data payload is with schema fileName, yCoord, xCoord, time, forecast, measureName, measureValue
         */
        String schema = AppConfig.DB_TABLE_DEFAULT_SCHEMA; // TODO dynamic schema

        List<StructField> fields = new ArrayList<StructField>();
        for (String type : schema.split(",")) {
            String[] value = type.split(":");
            StructField field = null;
            if (value[1].equalsIgnoreCase("string")) {
                field = DataTypes.createStructField(type.split(":")[0], DataTypes.StringType, true);
            } else if (value[1].equalsIgnoreCase("double")) {
                field = DataTypes.createStructField(type.split(":")[0], DataTypes.DoubleType, true);
            }
            fields.add(field);
        }
        org.apache.spark.sql.types.StructType type = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRdd = msgRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String arg0) {
                String[] data = arg0.split(",");
                return RowFactory.create(
                        data[0],
                        Double.parseDouble(data[1]),
                        Double.parseDouble(data[2]),
                        Double.parseDouble(data[3]),
                        Double.parseDouble(data[4]),
                        data[5],
                        Double.parseDouble(data[6]));
            }
        });

        SparkSession session = SparkSession.builder().appName(appName + " - 2. NetCDF dumping").master(sparkMaster)
                .getOrCreate();
        /**
         * Covert rdd to dataset and dump to database
         */
        Dataset<Row> ds = session.createDataFrame(rowRdd.rdd(), type);

        ds.printSchema();
        ds.show();
        System.out.println("DS COUNT = " +  ds.count());

        Properties prop = new java.util.Properties();
        prop.setProperty("user", AppConfig.DB_USER);
        prop.setProperty("password", AppConfig.DB_PWD);
        prop.setProperty("driver", "com.mysql.jdbc.Driver");

        ds.write()
                .mode(org.apache.spark.sql.SaveMode.Append)
                .option("numPartitions", Integer.parseInt(dbProperties.getProperty("numPartitions", "5")))
                .jdbc(dbProperties.getProperty("url"), dbProperties.getProperty("table"), dbProperties);

        //rowRdd.saveAsTextFile("file:///Users/dadu/Git/big_data_homework/output/00001b1de284ad76ff1498690261c8cd8cb2b197_out");
        javaSparkContext.close();
        session.close();
    }

    /**
     * Read one NetCDF in each spark job. Extract data and dump to the databaes.
     * @param sparkMaster running mode
     * @param appName application name
     * @param fileNamePath where to read the file
     * @param keyMeasure key variable name to extract
     * @param coordinate a comma separated string to specify coordination system, such as latitude,longitude or projection_y_coordinate,projection_x_coordinate
     * @param realizationLimit the number of realization to read. -1 reads all.
     * @param dbProperties database property
     */
    public void readOneFileAndDump(String sparkMaster, String appName, String fileNamePath, String keyMeasure,
                            String coordinate, int realizationLimit, Properties dbProperties) {
        readListFilesAndDump(sparkMaster, appName,
                Arrays.asList(fileNamePath + "," + keyMeasure + "," + coordinate + "," + realizationLimit),
                dbProperties
        );
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\dadu\\Soft\\hadoopConfig"); // for windows only

        Properties prop = new java.util.Properties();
        prop.setProperty("user", AppConfig.DB_USER);
        prop.setProperty("password", AppConfig.DB_PWD);
        prop.setProperty("driver", AppConfig.DB_DRIVER);
        prop.setProperty("url", AppConfig.DB_JDBC_URL);
        prop.setProperty("numPartitions", "8");
        prop.setProperty("table", "air_pressure_at_sea_level");

        SparkNETCDFReader cdf = new SparkNETCDFReader();
        cdf.readOneFileAndDump(
                "local[*]",
                "Demo",
                "air_pressure_at_sea_level.nc",
                "air_pressure_at_sea_level",
                "projection_y_coordinate,projection_x_coordinate",
                1,
                prop
        );

        cdf.readListFilesAndDump(
                "local[*]",
                "Demo",
                Arrays.asList(
                        "air_pressure_at_sea_level.nc,air_pressure_at_sea_level,projection_y_coordinate,projection_x_coordinate,1",
                        "air_pressure_at_sea_level.nc,air_pressure_at_sea_level,projection_y_coordinate,projection_x_coordinate,1"
                ),
                prop
        );
    }
}
