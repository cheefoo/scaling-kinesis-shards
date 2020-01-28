# AutoScaling Kinesis Shards using Kinesis Data Stream Metric IncomingRecords

### 1. Overview
This application is a proof of concept of how you can scale your Amazon Kinesis shards automatically using 
the Kinesis UpdateShardCount API from Lambda functions.



### 2. Architecture

![alt text](https://code.amazon.com/packages/Scalingkinesisshards/blobs/882210f4a2bea7c5c583afd9922d70aa8c476a29/--/KinesisAutoScaling.png?raw=1)


For these particular POC we used the Kinesis Metric Incoming Records and we set a cloudwatch alarm on this 
metric at a certain threshold. 

### 3. ScaleUpKinesisShards Lambda function
When this threshold is breached the cloudwatch alarm is triggered and it sends
an SNS message to the Lambda function which then executes the updateShardCount API by doubling the number of shards
on the stream and then updating the metric alarm threshold to twice its original value.


### 4. ScaleDownKinesisShards Lambda function
When this threshold is breached the cloudwatch alarm is triggered and it sends
an SNS message to the Lambda function which then executes the updateShardCount API by halfing the number of shards
on the stream and then updating the metric alarm threshold to half its original value.
 
 
 ### 5. Requirements
 1. AWS Account
 2. Amazon Kinesis Stream
 
 ### 6. Cloudformation Template Resources
 1. LambdaExecutionRole		AWS::IAM::Role	
 2. SNSSubscription		AWS::SNS::Subscription	
 3. SNSTopic	AWS::SNS::Topic	
 4. ScaleDownAlarm	AWS::CloudWatch::Alarm	
 5. ScaleDownKinesisShards	AWS::Lambda::Function	
 6. ScaleUpAlarm	AWS::CloudWatch::Alarm	
 7. ScaleUpKinesisShards	AWS::Lambda::Function	
 