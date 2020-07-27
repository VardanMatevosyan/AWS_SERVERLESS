package com.lambda.utils;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathResult;
import com.amazonaws.services.simplesystemsmanagement.model.Parameter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AWSSystemManager {
  private final AWSSimpleSystemsManagement awsSimpleSystemsManagement;

  public AWSSystemManager() {
    awsSimpleSystemsManagement = AWSSimpleSystemsManagementClientBuilder.defaultClient();
  }

  /**
   * Get parameter from SSM, with or without encryption (use IAM role for decryption)
   * Throws {@Link com.amazonaws.services.simplesystemsmanagement.model.ParameterNotFoundException} if not found
   * @param parameterName
   * @param encryption
   * @return value
   */
  public String getParameter(String parameterName, boolean encryption) {
    GetParameterRequest getparameterRequest = new GetParameterRequest().withName(parameterName).withWithDecryption(encryption);
    GetParameterResult result = awsSimpleSystemsManagement.getParameter(getparameterRequest);
    return result.getParameter().getValue();
  }

  /**
   * Get parameter from SSM by path, with or without encryption (use IAM role for decryption)
   * Returns Map of all values, with all path parameters removed, since we assume that the path is for environment
   * @param path
   * @param encryption
   * @return Map of all values in path
   */
  public Map<String, String> getParametersByPath(String path, boolean encryption) {
    GetParametersByPathRequest getParametersByPathRequest = new GetParametersByPathRequest().withPath(path)
            .withWithDecryption(encryption)
            .withRecursive(true);
    String token = null;
    Map<String, String> params = new HashMap<>();
    do {
      getParametersByPathRequest.setNextToken(token);
      GetParametersByPathResult parameterResult = awsSimpleSystemsManagement.getParametersByPath
              (getParametersByPathRequest);
      token = parameterResult.getNextToken();
      params.putAll(addParamsToMap(parameterResult.getParameters()));
    } while (token != null);
    return params;
  }

  private Map<String,String> addParamsToMap(List<Parameter> parameters) {
    return parameters.stream().map(param -> {
      int envSeparator = param.getName().indexOf("/",1);
      return new ImmutablePair<>(param.getName().substring(envSeparator+1), param.getValue());
    }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

}
