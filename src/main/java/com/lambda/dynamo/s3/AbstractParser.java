package com.lambda.dynamo.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;
import com.lambda.dynamo.GatewayResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractParser implements RequestHandler<S3EventNotification, GatewayResponse> {
  private static final AmazonS3 S3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_CENTRAL_1).build();
  private static final Logger LOGGER = LogManager.getLogger(AbstractParser.class);

  @Override
  public GatewayResponse handleRequest(S3EventNotification event, Context context) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put("X-Custom-Header", "application/json");

    StringBuilder builder = new StringBuilder();
    GatewayResponse responseString = new GatewayResponse("{}", headers, 200);
    S3EventNotification.S3Entity s3Entity;
    Optional<S3EventNotification.S3EventNotificationRecord> record = event.getRecords().stream().findFirst();
    if (record.isPresent()) {
      S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = record.get();
      s3Entity = s3EventNotificationRecord.getS3();
      String awsRegion = s3EventNotificationRecord.getAwsRegion();
      String bucketName = s3Entity.getBucket().getName();
      String key = s3Entity.getObject().getKey();

      LOGGER.debug(String.format("%s %s %s %s",
              "Bucket key and name region", key, bucketName, awsRegion));
      S3Object response = S3.getObject(bucketName, key);

      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.getObjectContent()));
      String line = "";

      try {
        while ((line = bufferedReader.readLine()) != null) {
          builder.append(line);
        }
        LOGGER.info(String.format("%s%s%s",
                "Build the json string", System.lineSeparator(), builder.toString()));
        List<Object> users = getUsers(builder.toString());
        responseString = this.save(users);
      } catch (IOException e) {
        e.printStackTrace();
        context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", bucketName, key));
        return new GatewayResponse(e.toString(), headers, 500);
      }
    }
    return responseString;
  }

  protected abstract GatewayResponse save(List<Object> users);
  protected abstract List<Object> getUsers(String body) throws IOException;
}
