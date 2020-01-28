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

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DoubleKinesisShards implements RequestHandler<SNSEvent, String>
{
    public static final String filePath = "/Users/temitayo/Desktop/KinesisSNSEventRecord1.json";
    private static AmazonKinesis kinesis;
    private static AmazonCloudWatch cloudwatch;
    @Override
    public String handleRequest(SNSEvent event, Context context)
    {
        context.getLogger().log("Input is " + event);

        //SNSEvent.SNSRecord record = event.getRecords().get(0);


        String message = event.getRecords().get(0).getSNS().getMessage();


        System.out.println("Message " + message);

        return null;
    }

    public static void main(String[] args) throws IOException, ParseException
    {
        if(args.length != 1 )
        {
            System.out.println("Please provide path to file");
            System.exit(1);
        }

        //String fileData = new String(Files.readAllBytes(Paths.get(args[0])));

        kinesis = AmazonKinesisClientBuilder.standard().build();
        cloudwatch = AmazonCloudWatchClientBuilder.standard().build();

        FileReader reader = new FileReader(args[0]);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);
        JSONArray recordsArray = (JSONArray)jsonObject.get("Records");

        JSONObject snsMessage = (JSONObject) ((JSONObject) recordsArray.get(0)).get("Sns");

       /* for(int i = 0; i < recordsArray.size(); i++)
        {
            System.out.println("record " + recordsArray.get(i));
        }

        Iterator iterator = recordsArray.iterator();
        JSONObject snsMessage = null;
        while(iterator.hasNext())
        {
            JSONObject jsonArrayObject = (JSONObject) iterator.next();
            snsMessage = (JSONObject) jsonArrayObject.get("Sns");
            System.out.println(snsMessage);
        }*/
        String snsMessageId = (String) snsMessage.get("MessageId");

        System.out.println(snsMessageId);

        JSONObject snsEventMessage = (JSONObject) snsMessage.get("Message");

        System.out.println("Real Dude " + snsEventMessage );

        JSONObject triggerStatistic = (JSONObject) snsEventMessage.get("Trigger");
        JSONArray triggerDimensionsArray = (JSONArray)triggerStatistic.get("Dimensions");
        System.out.println("D Name is " + triggerDimensionsArray.toJSONString());

        String streamName = (String) ((JSONObject) triggerDimensionsArray.get(0)).get("value");
        System.out.println("Stream Name is " + streamName);

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        StreamDescription streamDescription =  kinesis.describeStream(describeStreamRequest).getStreamDescription();
        int totalShardCount = streamDescription.getShards().size();
        System.out.println("Initial Total Shard Count " + totalShardCount);
        /*while(streamDescription.isHasMoreShards())
        {
            streamDescription =  kinesis.describeStream(describeStreamRequest).getStreamDescription();
        }*/

       /* JSONParser jsonStreamParser = new JSONParser();
        JSONObject jsonStreamObject = (JSONObject) jsonStreamParser.parse(streamDescription.toString());
        JSONArray jsonStreamArray = (JSONArray) jsonStreamObject.get("Shards");

        String lastShardId = (String)((JSONObject)jsonStreamArray.get(jsonStreamArray.size()-1)).get("ShardId");
        System.out.println("Last Shard Id " + lastShardId);
*/

        //Get second to last shard
        String lastShardIdForExclusiveStart = streamDescription.getShards().get(streamDescription.getShards().size()-1).getShardId();
        System.out.println("Last Shard Id is " + lastShardIdForExclusiveStart);
        System.out.printf("is it" + streamDescription.toString());

        DescribeStreamRequest describeStreamRequestWithExclusiveShardStart
                = new DescribeStreamRequest().withStreamName(streamName);
        String exclusiveStartShardId = null;
        List<Shard> shards = new ArrayList<>();
        describeStreamWithExclusiveStartShard(describeStreamRequestWithExclusiveShardStart, exclusiveStartShardId, shards);

        totalShardCount = shards.size();





        //double the threshold for the CloudWatch alarm that triggered this function
       /* PutMetricAlarmRequest putMetricAlarmRequest = new PutMetricAlarmRequest();

        putMetricAlarmRequest.setAlarmName((String)snsEventMessage.get("AlarmName"));

        String snsTopicArn = (String) snsMessage.get("TopicArn");
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
*/




      /* for(int j = 0; j < triggerDimensionsArray.size(); j++)
        {
            System.out.println("Trigger " + triggerDimensionsArray.get(j));
        }
        Iterator dimensionIterator = triggerDimensionsArray.iterator();

       String streamName = null;
        while(dimensionIterator.hasNext())
        {
            JSONObject jsonArrayObject = (JSONObject) dimensionIterator.next();
            streamName = (String) jsonArrayObject.get("value");
            //System.out.println(streamName);
        }

        System.out.println("Stream Name is " + streamName);*/
    }

    private static void describeStreamWithExclusiveStartShard(DescribeStreamRequest describeStreamRequestWithExclusiveShardStart, String exclusiveStartShardId, List<Shard> shards) {
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
    }
}
