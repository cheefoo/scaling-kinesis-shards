package com.aws;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;


public class SendRecordsToKinesisStream  implements RequestHandler<S3Event, String>
{
    private static final String region = "us-west-2";
    private static ThreadPoolExecutor kinesisThreadPool;
    private static final int KINESIS_THREADS = 10;
    private static final String STREAM_NAME = "DowIndexStream";
    private static AmazonKinesis kinesis;

    private synchronized static ThreadPoolExecutor getKinesisThreadPool() {
        if (kinesisThreadPool == null) {
            kinesisThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(KINESIS_THREADS);
        }
        return kinesisThreadPool;
    }


    @Override
    public String handleRequest(S3Event s3Event, Context context)
    {
        S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);

        String bucketName = record.getS3().getBucket().getName();
        System.out.println("Bucket Name is " + bucketName);
        String key = record.getS3().getObject().getKey().replace('+', ' ');
        System.out.println("Key is " + key);
        kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
        List<String> dataList = retrieveDataList(bucketName, key);

        Future future = getKinesisThreadPool().submit(() -> {
            try {
                putMultipleRecordBatch(STREAM_NAME, dataList, kinesis);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        return null;
    }

  /*  public static void main(String[] args)
    {
        String bucketName = "dfw-meetup-sf";
        System.out.println("Bucket Name is " + bucketName);
        String key = "kinesistest/stocks.csv";
        System.out.println("Key is " + key);
        kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
        List<String> dataList = retrieveDataList(bucketName, key);

        Future future = getKinesisThreadPool().submit(() -> {
            try {
                putMultipleRecordBatch(STREAM_NAME, dataList, kinesis);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


    }*/

    private static List<String> retrieveDataList(String bucketName, String key)
    {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        List dataList = new ArrayList();
        try
        {
            InputStreamReader inputStreamReader = new InputStreamReader(s3Client.getObject(bucketName, key).getObjectContent());
            BufferedReader br = new BufferedReader(inputStreamReader);
            String line = null;
            while((line = br.readLine()) != null)
            {
                dataList.add(line);
            }

        }
        catch(Exception e)
        {
            System.out.printf(e.toString());
        }

        return dataList;
    }

    private static void putMultipleRecordBatch(String myStreamName, List<String> dataList, AmazonKinesis kinesis) throws Exception
    {
        PutRecordsRequest putRecordsRequest;
        List<PutRecordsRequestEntry> putRecordsRequestEntryList;
        int batchRequestCount = 1;

        List<String> dataList2 =  dataList;
        while (true)
        {
            putRecordsRequestEntryList = new ArrayList<>();
            for(int i = 0; i < dataList.size(); i++ )
            {
                long createTime = System.currentTimeMillis();
                PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
                putRecordsRequestEntry.setData(ByteBuffer.wrap(dataList2.get(i).getBytes("UTF-8")));
                putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", createTime));
                putRecordsRequestEntryList.add(putRecordsRequestEntry);

                if(i%499==0)
                {
                    putRecordsRequest = new PutRecordsRequest();
                    putRecordsRequest.setStreamName(myStreamName);
                    putRecordsRequest.setRecords(putRecordsRequestEntryList);
                    PutRecordsResult putRecordsResult = kinesis.putRecords(putRecordsRequest) ;
                    System.out.println("Successfully sent put record batch number : " + batchRequestCount + " with result " + putRecordsResult.toString());
                    putRecordsRequestEntryList = new ArrayList<>();
                    batchRequestCount++;
                }

            }
            dataList2 = dataList;

        }
    }
}
