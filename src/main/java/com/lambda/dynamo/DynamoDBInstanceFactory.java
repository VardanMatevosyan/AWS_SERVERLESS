package com.lambda.dynamo;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.lambda.utils.AWSSystemManager;

public class DynamoDBInstanceFactory {
  private static final String ACCESS_KEY_NAME = "/parser/dev/aws_access_key";
  private static final String ACCESS_KEY_SECRET = "/parser/dev/aws_access_secret";
  private static AWSCredentialsProvider CREDENTIALS_PROVIDER;
  private static AmazonDynamoDB DYNAMO_DB_CLIENT;
  private static DynamoDB DYNAMO_DB;
  private static Table TABLE;
  private static DynamoDBMapperConfig CONFIG;
  private static DynamoDBMapper DYNAMO_DB_MAPPER;

  static {
    AWSSystemManager systemManager = new AWSSystemManager();
    String accessKey = systemManager.getParameter(ACCESS_KEY_NAME, false);
    String accessSecret = systemManager.getParameter(ACCESS_KEY_SECRET, false);

    CREDENTIALS_PROVIDER = new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(accessKey, accessSecret));
    DYNAMO_DB_CLIENT = AmazonDynamoDBClientBuilder
            .standard().withCredentials(CREDENTIALS_PROVIDER)
            .withRegion(DynamoConstants.REGION).build();
    DYNAMO_DB = new DynamoDB(DYNAMO_DB_CLIENT);
    TABLE = DYNAMO_DB.getTable(DynamoConstants.TABLE_NAME);

    CONFIG = DynamoDBMapperConfig
            .builder()
            .withTableNameResolver(new DynamoDBMapperConfig.DefaultTableNameResolver())
            .build();

    DYNAMO_DB_MAPPER =  new DynamoDBMapper(DYNAMO_DB_CLIENT, CONFIG);
  }

  public static Table getDynamoDBTableObject() {
    return TABLE;
  }

  public static AmazonDynamoDB getDynamoDBClient() {
    return DYNAMO_DB_CLIENT;
  }

  public static DynamoDBMapper getDynamoDbMapper() {
    return DYNAMO_DB_MAPPER;
  }

  public static DynamoDB getDynamoDB() {
    return DYNAMO_DB;
  }
}
