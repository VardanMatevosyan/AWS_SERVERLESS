package com.lambda.dynamo.s3.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lambda.dynamo.s3.AbstractParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lambda.dynamo.DynamoDBInstanceFactory;
import com.lambda.dynamo.GatewayResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ParserV4 extends AbstractParser {
  private static final DynamoDBMapper DYNAMO_DB_MAPPER = DynamoDBInstanceFactory.getDynamoDbMapper();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LogManager.getLogger(AbstractParser.class);

  @Override
  protected GatewayResponse save(List<Object> users) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put("X-Custom-Header", "application/json");

    List<User> userList = (List<User>) (Object) users;
    String response;
    List<DynamoDBMapper.FailedBatch> failedBatches = DYNAMO_DB_MAPPER.batchSave(userList);
    response = failedBatches.size() > 0 ? "{message : error}" : "{message : success}";
    return new GatewayResponse(response, headers, 200);
  }

  @Override
  protected List<Object> getUsers(String body) throws IOException {
    OBJECT_MAPPER.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
    List<User> users = OBJECT_MAPPER.readValue(body, new TypeReference<List<User>>(){});
    users.forEach(a -> {
      LOGGER.info(String.format("%s User with id = %s, name = %s",
              "Build the list of users test 1", a.getId(), a.getName()));
    });
    return (List<Object>)(Object)users;
  }

  private User getUser(String id) {
    DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
            .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
            .build();
    User load = DYNAMO_DB_MAPPER.load(User.class, id, config);
    return load;
  }
}
