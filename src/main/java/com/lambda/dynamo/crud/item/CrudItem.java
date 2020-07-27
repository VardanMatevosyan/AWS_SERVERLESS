package com.lambda.dynamo.crud.item;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
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

public class CrudItem implements RequestHandler<Map<String, String>, GatewayResponse> {
  private static final Table TABLE = DynamoDBInstanceFactory.getDynamoDBTableObject();
  private static final Logger LOGGER = LogManager.getLogger(AbstractParser.class);
  private static final String CREATE = "create";
  private static final String READ = "read";
  private static final String UPDATE = "update";
  private static final String DELETE = "delete";
  private static final String OPERATION = "operation";


  @Override
  public GatewayResponse handleRequest(Map<String, String> input, Context context) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put("X-Custom-Header", "application/json");
    String response = "{}";
    String operation = input.get(OPERATION);
    switch (operation) {
      case READ:
        LOGGER.info("getItem before");
        response = this.getItem();
        break;
      case CREATE:
        response = this.createItem();
        break;
      case UPDATE:
        response = this.updateItem();
        break;
      case DELETE:
        response = this.deleteItem();
        break;
    }
    return new GatewayResponse(response, headers, HttpStatus.SC_OK);
  }

  private String deleteItem() {
    DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey("id", "16")
            .withConditionExpression("#name = :value")
            .withNameMap(new NameMap().with("#name", "name"))
            .withValueMap(new ValueMap().with(":value", "New Item Created Name"))
            .withReturnValues(ReturnValue.ALL_OLD);
    DeleteItemOutcome deleteItemOutcome = TABLE.deleteItem(deleteItemSpec);
    return deleteItemOutcome.getItem().toJSONPretty();
  }

  private String updateItem() {
    UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("id", "15")
            .withUpdateExpression("set #name=:newValue")
            .withNameMap(new NameMap().with("#name", "name"))
            .withValueMap(new ValueMap().with(":newValue", "updated Value"))
            .withReturnValues(ReturnValue.ALL_NEW);
    UpdateItemOutcome updateItemOutcome = TABLE.updateItem(updateItemSpec);
    return updateItemOutcome.getItem().toJSONPretty();
  }

  private String createItem() {
    Item item = new Item()
            .withPrimaryKey(DynamoConstants.PARTITION_KEY_TEST_TABLE, "16")
            .withString("name", "New Item Created Name");
    PutItemSpec putItemSpec = new PutItemSpec()
            .withItem(item)
            .withReturnValues(ReturnValue.ALL_OLD);
    PutItemOutcome putItemOutcome = TABLE.putItem(putItemSpec);
    return putItemOutcome != null && putItemOutcome.getItem() != null
            ? String.format("{%s:%s - %s}", "message", "The item was replaced", putItemOutcome.getItem().toJSON())
            : String.format("{%s:%s}", "message", "The item was created");
  }

  private String getItem() {
    String response = "{}";
    Item item = TABLE.getItem("id", "15");
    LOGGER.info("Item JSON - " + item.toJSONPretty());
    response = item.toJSONPretty();
    return response;
  }
}
