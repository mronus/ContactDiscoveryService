/*
 * Copyright (C) 2018 Open Whisper Systems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.contactdiscovery.directory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.contactdiscovery.configuration.DirectorySqsConfiguration;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DirectoryQueue {

  private final Logger logger = LoggerFactory.getLogger(DirectoryQueue.class);

  private static final int VISIBILITY_TIMEOUT = 30;
  private static final int WAIT_TIME          = 20;

  private static final int RECEIVE_BATCH_SIZE = 10;

  private final AmazonSQS sqs;
  private final String    queueUrl;

  public DirectoryQueue(DirectorySqsConfiguration sqsConfig) {
    AWSCredentials               credentials         = new BasicAWSCredentials(sqsConfig.getAccessKey(), sqsConfig.getAccessSecret());
    AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

    this.sqs      = AmazonSQSClientBuilder.standard()
                                          .withRegion(sqsConfig.getQueueRegion())
                                          .withCredentials(credentialsProvider).build();
    this.queueUrl = sqsConfig.getQueueUrl();
  }

  public List<Message> waitForMessages() {
    ReceiveMessageRequest receiveMessageRequest =
        new ReceiveMessageRequest().withQueueUrl(queueUrl)
                                   .withMaxNumberOfMessages(RECEIVE_BATCH_SIZE)
                                   .withVisibilityTimeout(VISIBILITY_TIMEOUT)
                                   .withMessageAttributeNames("All")
                                   .withWaitTimeSeconds(WAIT_TIME);

    ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

    return receiveMessageResult.getMessages();
  }

  public Stream<String> deleteMessages(Set<String> messageReceipts) {
    if (messageReceipts.isEmpty()) {
      return Stream.empty();
    }

    List<DeleteMessageBatchRequestEntry> deletes =
        messageReceipts.stream()
                       .map(receipt -> new DeleteMessageBatchRequestEntry(receipt, receipt))
                       .collect(Collectors.toList());

    DeleteMessageBatchRequest request = new DeleteMessageBatchRequest(queueUrl, deletes);
    DeleteMessageBatchResult  result  = sqs.deleteMessageBatch(request);

    result.getFailed().stream().forEach(error -> {
      logger.error("error response deleting from directory queue: ", error.getMessage());
    });

    Stream<String> successReceipts = result.getSuccessful().stream()
                                           .map(DeleteMessageBatchResultEntry::getId);
    Stream<String> clientErrorReceipts = result.getFailed().stream()
                                               .filter(BatchResultErrorEntry::isSenderFault)
                                               .map(BatchResultErrorEntry::getId);

    return Stream.concat(successReceipts, clientErrorReceipts);
  }

}
