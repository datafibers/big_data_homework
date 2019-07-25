package com.datafibers.aws.homework;

import java.util.ArrayList;
import java.util.List;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/**
 * AWS Simple Queue Service (SQS) Client to read message
 */
public class SQSClient {

    /**
     * Get client to read from SQS
     * @param awsCredentials credential
     * @param regions sqs region
     * @return sqs client
     */
    public static AmazonSQS getClient(AWSCredentials awsCredentials, Regions regions) {
        return AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(regions)
                .build();
    }

    /**
     * Read specified number of messages from the SQS
     * @param sqsClient sqs client
     * @param sqsURL sqs URL
     * @param waitTime how long to wait till next read
     * @param numberOfMsg how many messages to read in one pool
     * @return List of messages to read
     */
    public static List<Message> readMsgs(AmazonSQS sqsClient, String sqsURL, int waitTime, int numberOfMsg) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsURL)
                .withWaitTimeSeconds(waitTime) // Long polling;
                .withMaxNumberOfMessages(numberOfMsg); // Max is 10

        return sqsClient.receiveMessage(receiveMessageRequest).getMessages();
    }

    /**
     * Read only one message in a pool
     * @param sqsClient sqs client
     * @param sqsURL sqs URL
     * @param waitTime how long to wait till next read
     * @return a message read
     */
    public static Message readAMsg(AmazonSQS sqsClient, String sqsURL, int waitTime) {
        return readMsgs(sqsClient, sqsURL, waitTime, 1).get(0);
    }

    /**
     * Delete a message from sqs
     * @param sqsClient sqs client
     * @param sqsURL sqs URL
     * @param message what to delete
     */
    public static void deleteAMsg(AmazonSQS sqsClient, String sqsURL, Message message) {
        sqsClient.deleteMessage(new DeleteMessageRequest(sqsURL, message.getReceiptHandle()));
    }

    /**
     * Read one message in json format
     * @param sqsClient sqs client
     * @param sqsURL sqs URL
     * @param waitTime how long to wait till next read
     * @return a message read in json
     */
    public static JSONObject readAMsgAsJson(AmazonSQS sqsClient, String sqsURL, int waitTime) {
        final Message message = readMsgs(sqsClient, sqsURL, waitTime, 1).get(0);
        return new JSONObject(new JSONObject(message.getBody())
                .getString("Message")
                .replace("\\\\", ""));
    }

    public static List<JSONObject> readMsgsAsJson(AmazonSQS sqsClient, String sqsURL, int waitTime, int numberOfMsg) {
        List<JSONObject> jsonObjList = new ArrayList<>();
        final List<Message> messages = readMsgs(sqsClient, sqsURL, waitTime, numberOfMsg);
        for(Message msg : messages) {
            jsonObjList.add(new JSONObject(new JSONObject(msg.getBody()).getString("Message")
                    .replace("\\\\", "")));

        }
        return jsonObjList;
    }

    public static void main(String[] args) {
        System.out.println(
                SQSClient.readAMsgAsJson(
                        SQSClient.getClient(AppConfig.CREDENTIALS, AppConfig.SQS_REGIONS),
                        AppConfig.SQS_URL,
                        10
                )
        );

        System.out.println(
                StringUtils.join("|",
                    SQSClient.readMsgs(
                            SQSClient.getClient(AppConfig.CREDENTIALS, AppConfig.SQS_REGIONS),
                            AppConfig.SQS_URL,
                            10,
                            5
                    )
                )
        );
    }
}
