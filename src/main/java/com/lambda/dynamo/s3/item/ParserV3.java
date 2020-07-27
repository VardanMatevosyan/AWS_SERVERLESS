package com.lambda.dynamo.s3.item;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lambda.dynamo.s3.AbstractParser;
import com.lambda.dynamo.DynamoConstants;
import com.lambda.dynamo.DynamoDBInstanceFactory;
import com.lambda.dynamo.GatewayResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParserV3 extends AbstractParser {
  private static final Table TABLE = DynamoDBInstanceFactory.getDynamoDBTableObject();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LogManager.getLogger(AbstractParser.class);

  @Override
  protected GatewayResponse save(List<Object> users) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put("X-Custom-Header", "application/json");

    List<User> userList = (List<User>) (Object) users;
    userList.forEach(user -> {
      Item item = new Item()
              .withPrimaryKey(DynamoConstants.PARTITION_KEY_TEST_TABLE, user.getId());
      item.with("name", user.getName());
      TABLE.putItem(item);
    });
    return new GatewayResponse("{message : success}", headers, 200);
  }

  @Override
  protected List<Object> getUsers(String body) throws IOException {
    OBJECT_MAPPER.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
    List<User> users = OBJECT_MAPPER.readValue(body, new TypeReference<List<User>>(){});
    users.forEach(a -> {
      LOGGER.info(String.format("%s User id = %s, name = %s",
              "Build the list of users", a.getId(), a.getName()));
    });
    return (List<Object>)(Object)users;
  }
}
