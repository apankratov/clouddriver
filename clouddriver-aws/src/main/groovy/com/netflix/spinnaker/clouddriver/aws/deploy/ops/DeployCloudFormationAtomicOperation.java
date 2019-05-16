/*
 * Copyright (c) 2019 Schibsted Media Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.clouddriver.aws.deploy.ops;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.clouddriver.aws.deploy.description.DeployCloudFormationDescription;
import com.netflix.spinnaker.clouddriver.aws.security.AmazonClientProvider;
import com.netflix.spinnaker.clouddriver.data.task.Task;
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository;
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DeployCloudFormationAtomicOperation implements AtomicOperation<Map> {

  private static final String BASE_PHASE = "DEPLOY_CLOUDFORMATION_STACK";

  @Autowired
  AmazonClientProvider amazonClientProvider;

  @Autowired
  @Qualifier("amazonObjectMapper")
  private ObjectMapper objectMapper;

  private DeployCloudFormationDescription description;

  public DeployCloudFormationAtomicOperation(DeployCloudFormationDescription deployCloudFormationDescription) {
    this.description = deployCloudFormationDescription;
  }

  @Override
  public Map operate(List priorOutputs) {
    Task task = TaskRepository.threadLocalTask.get();
    task.updateStatus(BASE_PHASE, "Configuring CloudFormation Stack " + description.getStackName());
    AmazonCloudFormation amazonCloudFormation = amazonClientProvider.getAmazonCloudFormation(
      description.getCredentials(), description.getRegion());
    String template;
    try {
      template = objectMapper.writeValueAsString(description.getTemplateBody());
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Could not serialize CloudFormation Stack template body", e);
    }
    List<Parameter> parameters = description.getParameters().entrySet().stream()
      .map(entry -> new Parameter()
        .withParameterKey(entry.getKey())
        .withParameterValue(entry.getValue()))
      .collect(Collectors.toList());
    List<Tag> tags = description.getTags().entrySet().stream()
      .map(entry -> new Tag()
        .withKey(entry.getKey())
        .withValue(entry.getValue()))
      .collect(Collectors.toList());

    Optional<String> stackId = Optional.empty();
    Optional<Stack> stack = getStack(amazonCloudFormation);
    if (stack.isPresent()) {
      StackStatus stackStatus = StackStatus.fromValue(stack.get().getStackStatus());
      switch (stackStatus) {
        case CREATE_FAILED:
        case ROLLBACK_COMPLETE:
          task.updateStatus(BASE_PHASE, "Stack exists, but is not updatable, deleting and re-creating.");
          if (deleteStack(amazonCloudFormation)) {
            stackId = createStack(amazonCloudFormation, template, parameters, tags, description.getCapabilities());
          }
          break;
        case CREATE_COMPLETE:
        case UPDATE_ROLLBACK_COMPLETE:
        case UPDATE_COMPLETE:
          stackId = updateStack(amazonCloudFormation, template, parameters, tags, description.getCapabilities());
          break;
        case ROLLBACK_FAILED:
        case DELETE_FAILED:
        case UPDATE_ROLLBACK_FAILED:
          task.updateStatus(BASE_PHASE, "Stack is stuck, manual actions are required in AWS console.");
          task.fail();
          break;
      }
    } else {
      stackId = createStack(amazonCloudFormation, template, parameters, tags, description.getCapabilities());
    }
    if (stackId.isPresent()) {
      return Collections.singletonMap("stackId", stackId.get());
    } else {
      return Collections.singletonMap("stackId", "");
    }
  }

  private boolean deleteStack(AmazonCloudFormation amazonCloudFormation) {
    Task task = TaskRepository.threadLocalTask.get();
    try {
      task.updateStatus(BASE_PHASE, "Deleting CloudFormation Stack");
      DeleteStackRequest deleteStackRequest = new DeleteStackRequest()
        .withStackName(description.getStackName());
      DeleteStackResult deleteStackResult = amazonCloudFormation.deleteStack(deleteStackRequest);
      task.updateStatus(BASE_PHASE, "Stack " + description.getStackName() + " deleted.");
      return true;
    } catch (AmazonCloudFormationException e) {
      task.updateStatus(BASE_PHASE, "Stack deletion failed.");
      task.fail();
      return false;
    }
  }

  private Optional<Stack> getStack(AmazonCloudFormation amazonCloudFormation) {
    Task task = TaskRepository.threadLocalTask.get();
    DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest()
      .withStackName(description.getStackName());
    DescribeStacksResult describeStacksResult;
    task.updateStatus(BASE_PHASE, "Getting current CloudFormation Stack status");
    try {
      describeStacksResult = amazonCloudFormation.describeStacks(describeStacksRequest);
      return Optional.of(describeStacksResult
        .getStacks()
        .stream()
        .findFirst()
        .get());
    } catch (AmazonCloudFormationException e) {
      task.updateStatus(BASE_PHASE, "Stack doesn't exist.");
      return Optional.empty();
    }
  }

  private Optional<String> createStack(AmazonCloudFormation amazonCloudFormation, String template, List<Parameter> parameters,
                             List<Tag> tags, List<String> capabilities) {
    Task task = TaskRepository.threadLocalTask.get();
    try {
      task.updateStatus(BASE_PHASE, "Creating CloudFormation Stack");
      CreateStackRequest createStackRequest = new CreateStackRequest()
        .withStackName(description.getStackName())
        .withParameters(parameters)
        .withTags(tags)
        .withTemplateBody(template)
        .withCapabilities(capabilities);
      CreateStackResult createStackResult = amazonCloudFormation.createStack(createStackRequest);
      task.updateStatus(BASE_PHASE, "Stack successfully created.");
      return Optional.of(createStackResult.getStackId());
    } catch (AmazonCloudFormationException e) {
      task.updateStatus(BASE_PHASE, "Stack creation failed.");
      task.fail();
      return Optional.empty();
    }
  }

  private Optional<String> updateStack(AmazonCloudFormation amazonCloudFormation, String template, List<Parameter> parameters,
                             List<Tag> tags, List<String> capabilities) {
    Task task = TaskRepository.threadLocalTask.get();
    try {
      task.updateStatus(BASE_PHASE, "Updating stack.");
      UpdateStackRequest updateStackRequest = new UpdateStackRequest()
        .withStackName(description.getStackName())
        .withParameters(parameters)
        .withTags(tags)
        .withTemplateBody(template)
        .withCapabilities(capabilities);
      UpdateStackResult updateStackResult = amazonCloudFormation.updateStack(updateStackRequest);
      task.updateStatus(BASE_PHASE, "Stack successfully updated.");
      return Optional.of(updateStackResult.getStackId());
    } catch (AmazonCloudFormationException e) {
      task.updateStatus(BASE_PHASE, "Stack update failed.");
      task.fail();
      return Optional.empty();
    }
  }
}
