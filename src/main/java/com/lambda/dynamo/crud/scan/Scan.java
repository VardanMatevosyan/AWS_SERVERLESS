package com.lambda.dynamo.crud.scan;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.lambda.dynamo.DynamoConstants;
import com.lambda.dynamo.DynamoDBInstanceFactory;
import com.lambda.dynamo.GatewayResponse;
import com.lambda.dynamo.s3.AbstractParser;
import org.apache.http.HttpStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class Scan implements RequestHandler<Map<String, String>, GatewayResponse> {

  private static final Logger LOGGER = LogManager.getLogger(AbstractParser.class);
  private final AmazonDynamoDB client = DynamoDBInstanceFactory.getDynamoDBClient();

  @Override
  public GatewayResponse handleRequest(Map<String, String> input, Context context) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put("X-Custom-Header", "application/json");
    return new GatewayResponse("success", headers, HttpStatus.SC_OK);
  }

  private void scanItems() {
    Map<String, AttributeValue> lastKeyEvaluated = null;
    do {
      ScanRequest scanRequest = new ScanRequest()
          .withTableName(DynamoConstants.TABLE_NAME)
          .withLimit(2)
          .withExclusiveStartKey(lastKeyEvaluated);

      LOGGER.info("Trying to get items");

      ScanResult result = client.scan(scanRequest);
      for (Map<String, AttributeValue> item : result.getItems()) {
        LOGGER.info("scanned item" + item);
      }
      lastKeyEvaluated = result.getLastEvaluatedKey();
    } while (lastKeyEvaluated != null);

  }

}
