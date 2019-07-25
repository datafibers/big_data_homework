package com.datafibers.aws.homework;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * Read or download files from AWS S3 with proper credentials, bucket number, and key/file name.
 */
public class S3Client {

    /**
     * Get a s3 client
     * @param awsCredentials key/password to access s3
     * @param regions s3 region
     * @return s3 client
     */
    public static AmazonS3 getClient(AWSCredentials awsCredentials, Regions regions) {
        return  AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(regions)
                .build();
    }

    /**
     * Download files from s3 with proper bucket name and file key
     * @param amazonS3 s3 client
     * @param s3BucketName bucket name
     * @param s3Key file name
     * @param downloadDir local where to save the download file
     */
    public static void downloadFiles(AmazonS3 amazonS3, String s3BucketName, String s3Key, String downloadFileNamePath) {
        try {
            FileUtils.copyInputStreamToFile(
                    amazonS3.getObject(s3BucketName, s3Key).getObjectContent(),
                    new File(downloadFileNamePath)
            );
        } catch (IOException ioe) {
            System.out.println("S3 download exception at " + ioe.getMessage());
        }
    }

    public static void main(String[] args) {
        String fileKey = "0e6c722bd0f0034746db864ffb39b2391d862337.nc";
        S3Client.downloadFiles(
                S3Client.getClient(AppConfig.CREDENTIALS, AppConfig.S3_REGIONS),
                AppConfig.S3_BUCKET,
                fileKey,
                AppConfig.S3_DOWNLOAD_DIR + "/" + fileKey
        );
    }
}
