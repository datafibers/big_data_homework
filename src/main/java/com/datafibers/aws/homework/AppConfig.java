package com.datafibers.aws.homework;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

public final class AppConfig {
    public static final AWSCredentials CREDENTIALS;
    public static final String SQS_URL = "https://sqs.us-east-2.amazonaws.com/aws-sqs-name";
    public static final String S3_BUCKET = "aws-s3-bucket-name";
    public static final Regions S3_REGIONS = Regions.EU_WEST_2;
    public static final Regions SQS_REGIONS = Regions.US_EAST_2;
    public static final String S3_DOWNLOAD_DIR = "/tmp/output";

    public static final String DB_HOST = "db ip address";
    public static final int DB_PORT = 3306;
    public static final String DB_NAME = "df_demo";
    public static final String DB_USER = "root";
    public static final String DB_PWD = "???";
    public static final String DB_TABLE = "air_pressure_at_sea_level";
    public static final String DB_JDBC_URL =
            "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" +
            DB_NAME + "?user=" + DB_USER + "&password=" + DB_PWD +"&rewriteBatchedStatements=true";
    public static final String DB_DRIVER = "com.mysql.jdbc.Driver";
    public static final String DB_WRITER_PAR = "8";
    public static final String DB_TABLE_DEFAULT_SCHEMA = "file_name:string,y_coord:double,x_coord:double,forecast:double,time:double,measure_name:string,measure_value:double";

    static {
        // put your accesskey and secretkey here
        CREDENTIALS = new BasicAWSCredentials(
                "aws_key",
                "aws_secret"
        );
    }
}
