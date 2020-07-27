package com.lambda.log;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.lambda.s3.ParserV2;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ParserV1 implements RequestHandler<S3EventNotification, String> {
  private static final Logger LOGGER = LogManager.getLogger(ParserV2.class);
  @Override
  public String handleRequest(S3EventNotification event, Context context) {
    LOGGER.info(String.format("%s%s%s",
            "Lambda function is invoked", System.lineSeparator(), event.toJson()));
    return event.toJson();
  }
}
