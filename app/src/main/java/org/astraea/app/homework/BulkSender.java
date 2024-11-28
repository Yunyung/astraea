/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.homework;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.app.argument.DataSizeField;
import org.astraea.app.argument.StringListField;
import org.astraea.common.DataSize;
import org.astraea.common.admin.AdminConfigs;

public class BulkSender {
  public static void main(String[] args) throws IOException, InterruptedException {
    execute(Argument.parse(new Argument(), args));
  }

  public static void execute(final Argument param) throws IOException, InterruptedException {
    int numPartitions = 8;
    // you must create topics for best configs
    try (var admin =
        Admin.create(Map.of(AdminConfigs.BOOTSTRAP_SERVERS_CONFIG, param.bootstrapServers()))) {
      for (var t : param.topics) {
        admin.createTopics(List.of(new NewTopic(t, numPartitions, (short) 1))).all();
      }
    }

    long totalDataSize = param.dataSize.bytes();
    int numTopics = param.topics.size(); // The number of topics
    int numProducers =
        Math.min(
            Runtime.getRuntime().availableProcessors(),
            numPartitions * numTopics); // The number of cores
    // long perProducerDataSize = totalDataSize / numProducers;
    System.out.println("numProducers = " + numProducers);
    // System.out.println("perProducerDataSize = " + perProducerDataSize);
    System.out.println("param.topics.size() = " + numTopics);
    System.out.println("param.dataSize.bytes() = " + totalDataSize);

    Map<String, Object> producerConfigs =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            param.bootstrapServers(),
            ProducerConfig.ACKS_CONFIG,
            "1",
            ProducerConfig.COMPRESSION_TYPE_CONFIG,
            "zstd",
            ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG,
            "-7",
            ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG,
            "true",
            ProducerConfig.BATCH_SIZE_CONFIG,
            "200000",
            ProducerConfig.LINGER_MS_CONFIG,
            100);

    // Shared atomic counter to track total sent data
    ExecutorService executorService = Executors.newFixedThreadPool(numProducers);
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicLong totalSentSize = new AtomicLong(0);
    for (int i = 0; i < numProducers; i++) {
      final int producerId = i;
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                try (var producer =
                    new KafkaProducer<>(
                        producerConfigs, new StringSerializer(), new StringSerializer())) {
                  var key = "key";
                  var value = "value";
                  while (totalSentSize.get() < totalDataSize) {
                    // Distribute messages across topics in a round-robin fashion
                    String topic = param.topics.get((producerId + futures.size()) % numTopics);

                    producer.send(
                        new ProducerRecord<>(topic, key, value),
                        (m, e) -> {
                          if (e == null) {
                            long messageSize = m.serializedKeySize() + m.serializedValueSize();
                            totalSentSize.addAndGet(messageSize);
                          }
                        });
                  }
                }
              },
              executorService);

      futures.add(future);
    }
    // Wait for all producers to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    // Shutdown executor service
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);

    System.out.println("Total data sent: " + totalSentSize.get() + " bytes");
  }

  // you must manage producers for best performance
  //    try (var producer =
  //        new KafkaProducer<>(producerConfigs, new StringSerializer(), new StringSerializer())) {
  //      var size = new AtomicLong(0);
  //      var key = "key";
  //      var value = "value";
  //      while (size.get() < totalDataSize) {
  //        tp = (tp + 1) % numTopics;
  //        var topic = param.topics.get(tp);
  //        producer.send(
  //            new ProducerRecord<>(topic, key, value),
  //            (m, e) -> {
  //              if (e == null) size.addAndGet(m.serializedKeySize() + m.serializedValueSize());
  //            });
  //      }
  //    }

  public static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you should send",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        required = true)
    List<String> topics;

    @Parameter(
        names = {"--dataSize"},
        description = "data size: total size you have to send",
        validateWith = DataSizeField.class,
        converter = DataSizeField.class)
    DataSize dataSize = DataSize.GB.of(10);
  }
}
