package com.aws;

/*
Copyright 2011-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License"). You
may not use this file except in compliance with the License. A copy of
the License is located at http://aws.amazon.com/apache2.0/
or in the "license" file accompanying this file. This file is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.util.List;

import static com.aws.Helper.getJsonObject;
import static com.aws.Helper.getOpenShards;
import static com.aws.Helper.updateCloudwatchMetricAlarm;

public class ScaleDownKinesisShards implements RequestHandler<SNSEvent, String>
{
    private static AmazonKinesis kinesis;
    private static AmazonCloudWatch cloudwatch;
    //private static final double multiplier = 0.5d;

    @Override
    public String handleRequest(SNSEvent event, Context context)
    {
        context.getLogger().log("Input is " + event);
        String message = event.getRecords().get(0).getSNS().getMessage();
        System.out.println("Message " + message);

        //Retrieve JSON object from message in SNS trigger
        JSONObject jsonObject = getJsonObject(message);
        JSONObject triggerStatistic = (JSONObject) jsonObject.get("Trigger");
        JSONArray triggerDimensionsArray = (JSONArray)triggerStatistic.get("Dimensions");
        System.out.println("Name is " + triggerDimensionsArray.toJSONString());
        String streamName = (String) ((JSONObject) triggerDimensionsArray.get(0)).get("value");
        System.out.println("Stream Name is " + streamName);


        kinesis = AmazonKinesisClientBuilder.standard().build();
        cloudwatch = AmazonCloudWatchClientBuilder.standard().build();

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        StreamDescription streamDescription =  kinesis.describeStream(describeStreamRequest).getStreamDescription();
        int totalShardCount = streamDescription.getShards().size();
        System.out.println("Initial Total Shard Count " + totalShardCount);
        DescribeStreamRequest describeStreamRequestWithExclusiveShardStart
                = new DescribeStreamRequest().withStreamName(streamName);
        String exclusiveStartShardId = null;
        List<Shard> shards = getOpenShards(describeStreamRequestWithExclusiveShardStart, exclusiveStartShardId, kinesis);
        totalShardCount = shards.size();
        System.out.println("Total Shard count " + totalShardCount);

        int openShardsCount = Helper.getOpenShardCount(shards);
        int targetShardsCount = openShardsCount/2;
        System.out.println("Proposed Target Shard Count using count of open shards " + targetShardsCount);

        //half the shard size with UpdateShardCount
        UpdateShardCountRequest updateShardCountRequest = new UpdateShardCountRequest();
        updateShardCountRequest.setStreamName(streamName);
        updateShardCountRequest.setScalingType("UNIFORM_SCALING");
        updateShardCountRequest.setTargetShardCount(targetShardsCount);
        UpdateShardCountResult updateShardCountResult = kinesis.updateShardCount(updateShardCountRequest);

        System.out.println("Updated shard count completed ...");
        System.out.println("Targeted Shard Count is " + updateShardCountResult.getTargetShardCount());
        Double multiplier = Double.valueOf(System.getenv("scale_down_multiplier"));
        if(updateShardCountResult.getTargetShardCount() == targetShardsCount)
        {
            //half the threshold for the CloudWatch alarm that triggered this function
            updateCloudwatchMetricAlarm(event, jsonObject, triggerStatistic, triggerDimensionsArray, cloudwatch, multiplier);
        }
        else
        {
            System.out.println("Exception updating shard count");
        }

        return null;
    }

}
