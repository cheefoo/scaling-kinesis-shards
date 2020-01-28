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
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;

public class Helper
{
     static void updateCloudwatchMetricAlarm(SNSEvent event, JSONObject jsonObject,
                                             JSONObject triggerStatistic, JSONArray triggerDimensionsArray,
                                             AmazonCloudWatch cloudwatch, double multiplier) {
        PutMetricAlarmRequest putMetricAlarmRequest = new PutMetricAlarmRequest();

        putMetricAlarmRequest.setAlarmName((String)jsonObject.get("AlarmName"));

        String snsTopicArn = event.getRecords().get(0).getSNS().getTopicArn();
        List<String> alarmActionsList = new ArrayList<>();
        alarmActionsList.add(snsTopicArn);
        putMetricAlarmRequest.setAlarmActions(alarmActionsList);

        putMetricAlarmRequest.setMetricName((String)triggerStatistic.get("MetricName"));
        putMetricAlarmRequest.setNamespace((String)triggerStatistic.get("Namespace"));

        String statistic = ((String)triggerStatistic.get("Statistic")).toLowerCase();
        String statisticEnum = statistic.substring(0,1).toUpperCase()+statistic.substring(1);
        System.out.println("capitalize first" + statisticEnum);
        putMetricAlarmRequest.setStatistic(statisticEnum);
        System.out.println("capitalize first" + statisticEnum);
        Dimension streamDimension = new Dimension();
        streamDimension.setName((String) ((JSONObject) triggerDimensionsArray.get(0)).get("name"));
        streamDimension.setValue((String) ((JSONObject) triggerDimensionsArray.get(0)).get("value"));
        List<Dimension> dimensionList = new ArrayList<>();
        dimensionList.add(streamDimension);
        putMetricAlarmRequest.setDimensions(dimensionList);

        Long period = (Long)triggerStatistic.get("Period");
        Long evalPeriod = (Long)triggerStatistic.get("EvaluationPeriods");
        putMetricAlarmRequest.setPeriod(period.intValue());
        putMetricAlarmRequest.setEvaluationPeriods(evalPeriod.intValue());
        putMetricAlarmRequest.setThreshold((Double)triggerStatistic.get("Threshold")*multiplier);
        putMetricAlarmRequest.setComparisonOperator((String)triggerStatistic.get("ComparisonOperator"));

        cloudwatch.putMetricAlarm(putMetricAlarmRequest);
        System.out.println();
    }

     static JSONObject getJsonObject(String message) {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = null;
        try
        {
            jsonObject = (JSONObject) jsonParser.parse(message);
        }
        catch (ParseException e)
        {
            System.out.println("Error parsing message , EXITING");
            e.printStackTrace();
            System.exit(1);
        }
        return jsonObject;
    }

     static List<Shard> getOpenShards(DescribeStreamRequest describeStreamRequestWithExclusiveShardStart,
                                      String exclusiveStartShardId, AmazonKinesis kinesis) {
        List<Shard> shards = new ArrayList<>();
        do {
            describeStreamRequestWithExclusiveShardStart.setExclusiveStartShardId(exclusiveStartShardId);
            DescribeStreamResult describeStreamResult = kinesis.describeStream(describeStreamRequestWithExclusiveShardStart);
            shards.addAll(describeStreamResult.getStreamDescription().getShards());
            if(describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0)
            {
                exclusiveStartShardId = shards.get(shards.size()-1).getShardId();
            }
            else
            {
                exclusiveStartShardId = null;
            }

        }while (exclusiveStartShardId != null);

        return shards;
    }

    public static int getOpenShardCount(List<Shard> shards)
    {
        int openShardsCount = 0;
        for (int i=0; i < shards.size(); i++)
        {
            Shard shard = shards.get(i);
            if(shard.getSequenceNumberRange().getEndingSequenceNumber() == null)
            {
                System.out.println("Shard number " + i +" with name " + shard.getShardId() + "is OPEN");
                openShardsCount++;
            }
            else
            {
                System.out.println("Shard number " + i +" with name " + shard.getShardId() + "is CLOSED");
            }
        }
        return openShardsCount;
    }
}
