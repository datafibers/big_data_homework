package com.datafibers.aws.homework;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.json.JSONObject;
import java.util.*;

public class NetCDFProcessDemo {
    public static final int DEMO_MSG_READ_BATCH_SIZE = 5;
    public static final int DEMO_FILE_DOWNLOAD_CNT = 4;

    /**
     * Poll one message in each pool and process one file
     * @param sparkMode mode
     * @param sparkAppName application name
     * @param fileParas one file parameters to extract the file
     * @param tartTableName which table to write in database
     */
    void demoOneFileOnePool(String sparkMode, String sparkAppName, String fileParas, String tartTableName) {
        AmazonSQS sqsClient = SQSClient.getClient(AppConfig.CREDENTIALS, AppConfig.SQS_REGIONS);
        AmazonS3 s3Client = S3Client.getClient(AppConfig.CREDENTIALS, AppConfig.S3_REGIONS);
        String[] fileParameters = fileParas.split(",");
        String filePrefixPara = fileParameters[0];
        String measurePara = fileParameters[1];
        String coordinationPara = fileParameters[2];
        int rLimitPara = Integer.parseInt(fileParameters[3]);

        // Read a message from a queue to get only 20 files to save storage
        int i = 0;
        while (i <= DEMO_FILE_DOWNLOAD_CNT) {
            JSONObject msg = SQSClient.readAMsgAsJson(sqsClient, AppConfig.SQS_URL, 10);
            String bucketAttr = msg.getString("bucket");
            String nametAttr = msg.getString("name");
            String keyAttr = msg.getString("key");
            String createdTimeAttr = msg.getString("created_time");
            String foreCastAttr = msg.getString("forecast_reference_time");

            System.out.println("bucket=" + bucketAttr);
            System.out.println("file_name=" + nametAttr);
            System.out.println("file_key=" + keyAttr);
            System.out.println("file_created_time=" + createdTimeAttr);
            System.out.println("forecast_reference_time=" + foreCastAttr);
            System.out.println("index=" + i);

            if (nametAttr.equalsIgnoreCase(filePrefixPara)) {
                //downloading an object to file
                String fileName = nametAttr + "_" + createdTimeAttr + "_" + keyAttr;
                String filePathName = AppConfig.S3_DOWNLOAD_DIR + "/" + fileName;

                S3Client.downloadFiles(s3Client, AppConfig.S3_BUCKET, keyAttr, filePathName);
                i++;
                System.out.println(fileName + " is downloaded and start processing ");

                Properties prop = new java.util.Properties();
                prop.setProperty("user", AppConfig.DB_USER);
                prop.setProperty("password", AppConfig.DB_PWD);
                prop.setProperty("driver", AppConfig.DB_DRIVER);
                prop.setProperty("url", AppConfig.DB_JDBC_URL);
                prop.setProperty("numPartitions", AppConfig.DB_WRITER_PAR);
                prop.setProperty("table", tartTableName);

                new SparkNETCDFReader().readOneFileAndDump(
                        sparkMode,
                        sparkAppName,
                        filePathName,
                        measurePara,
                        coordinationPara,
                        rLimitPara,
                        prop
                );
            }
        }
    }

    /**
     * Poll multiple message in each pool and process all file with same schema or same file into one table
     * @param sparkMode mode
     * @param sparkAppName application name
     * @param fileParas list of file parameters to extract the file
     * @param tartTableName which table to write in database
     */
    void demoMultipleFileOnePool(String sparkMode, String sparkAppName, Map<String, String> hmFileParas, String tartTableName) {
        AmazonSQS sqsClient = SQSClient.getClient(AppConfig.CREDENTIALS, AppConfig.SQS_REGIONS);
        AmazonS3 s3Client = S3Client.getClient(AppConfig.CREDENTIALS, AppConfig.S3_REGIONS);

        // Read a message from a queue to get only 20 files to save storage
        int i = 0;
        while (i <= DEMO_FILE_DOWNLOAD_CNT) {
            List<JSONObject> msgs = SQSClient.readMsgsAsJson(sqsClient, AppConfig.SQS_URL, 10, DEMO_MSG_READ_BATCH_SIZE);
            List<String> fileProcessParas = new LinkedList<>();
            for (JSONObject msg : msgs) {
                String bucketAttr = msg.getString("bucket");
                String nametAttr = msg.getString("name");
                String keyAttr = msg.getString("key");
                String createdTimeAttr = msg.getString("created_time");
                String foreCastAttr = msg.getString("forecast_reference_time");

                System.out.println("bucket=" + bucketAttr);
                System.out.println("file_name=" + nametAttr);
                System.out.println("file_key=" + keyAttr);
                System.out.println("file_created_time=" + createdTimeAttr);
                System.out.println("forecast_reference_time=" + foreCastAttr);
                System.out.println("index=" + i);

                if (hmFileParas.containsKey(nametAttr)) {
                    //downloading an object to file
                    String fileName = nametAttr + "_" + createdTimeAttr + "_" + keyAttr;
                    String filePathName = AppConfig.S3_DOWNLOAD_DIR + "/" + fileName;

                    S3Client.downloadFiles(s3Client, AppConfig.S3_BUCKET, keyAttr, filePathName);
                    i++;
                    System.out.println(fileName + " is downloaded and start processing ");
                    fileProcessParas.add(filePathName + "," + hmFileParas.get(nametAttr));
                }
            }

            // Only process when we have files match in one pool from sqs
            if(fileProcessParas.size() > 0) {
                Properties prop = new java.util.Properties();
                prop.setProperty("user", AppConfig.DB_USER);
                prop.setProperty("password", AppConfig.DB_PWD);
                prop.setProperty("driver", AppConfig.DB_DRIVER);
                prop.setProperty("url", AppConfig.DB_JDBC_URL);
                prop.setProperty("numPartitions", AppConfig.DB_WRITER_PAR);
                prop.setProperty("table", tartTableName);

                new SparkNETCDFReader().readListFilesAndDump(
                        sparkMode,
                        sparkAppName,
                        fileProcessParas,
                        prop
                );
            }
        }
    }

    public static void main(String[] args){
        Map<String, String> hmFileParas = new HashMap<>(); //key = fileMatchingPrefix, value = keyMeasure,coordY,coordX,rLimit
        hmFileParas.put("lwe_thickness_of_snowfall_amount", "lwe_thickness_of_snowfall_amount,projection_y_coordinate,projection_x_coordinate,1");
        hmFileParas.put("lwe_snowfall_rate", "lwe_snowfall_rate,projection_y_coordinate,projection_x_coordinate,1");

        NetCDFProcessDemo ncd = new NetCDFProcessDemo();

/*        ncd.demoOneFileOnePool(
                "local[*]",
                "NetCDF Process Demo 2",
                "air_pressure_at_sea_level" + hmFileParas.get("air_pressure_at_sea_level"),
                "air_pressure"
        );*/

        ncd.demoMultipleFileOnePool(
                "local[*]",
                "NetCDF Process Demo 1",
                hmFileParas,
                "snowfall_measures"
        );
    }
}
