package com.aws;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;

public class AutoScaleKinesisShards implements RequestHandler<SNSEvent, String>
{
    private static AmazonKinesis kinesis;
    private static AmazonCloudWatch cloudwatch;

    @Override
    public String handleRequest(SNSEvent event, Context context)
    {
        context.getLogger().log("Input is " + event);

        String message = event.getRecords().get(0).getSNS().getMessage();
        System.out.println("Message " + message);

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
        System.out.println("Did I exit");
        System.out.printf("No still working");

        JSONObject triggerStatistic = (JSONObject) jsonObject.get("Trigger");
        JSONArray triggerDimensionsArray = (JSONArray)triggerStatistic.get("Dimensions");
        System.out.println("D Name is " + triggerDimensionsArray.toJSONString());

        String streamName = (String) ((JSONObject) triggerDimensionsArray.get(0)).get("value");
        System.out.println("Stream Name is " + streamName);

        kinesis = AmazonKinesisClientBuilder.standard().build();
        cloudwatch = AmazonCloudWatchClientBuilder.standard().build();

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        StreamDescription streamDescription =  kinesis.describeStream(describeStreamRequest).getStreamDescription();
        int totalShardCount = streamDescription.getShards().size();
        System.out.println("Initial Total Shard Count " + totalShardCount);

        //Get second to last shard
        String lastShardIdForExclusiveStart = streamDescription.getShards().get(streamDescription.getShards().size()-1).getShardId();
        System.out.println("Last Shard Id is " + lastShardIdForExclusiveStart);
        System.out.printf("is it" + streamDescription.toString());

        DescribeStreamRequest describeStreamRequestWithExclusiveShardStart
                = new DescribeStreamRequest().withStreamName(streamName);
        String exclusiveStartShardId = null;
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

        totalShardCount = shards.size();

        System.out.println("Total Shard count " + totalShardCount);

        //double the threshold for the CloudWatch alarm that triggered this function
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
        putMetricAlarmRequest.setThreshold((Double)triggerStatistic.get("Threshold")*2);
        putMetricAlarmRequest.setComparisonOperator((String)triggerStatistic.get("ComparisonOperator"));

        cloudwatch.putMetricAlarm(putMetricAlarmRequest);

        return null;
    }
}
