using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaLib
{
    class ContentConsumer
    {
        private string _topicName;
        private ConsumerConfig _configs;

        public ContentConsumer(string topicName)
        {
            _topicName = topicName;

            _configs = new ConsumerConfig
            {
                GroupId = "watcher-consumer",
                BootstrapServers = "localhost:9092",
                EnableAutoCommit = false
            };
        }

        public void ConsumeMessages()
        {
            const int commitPeriod = 5;

            using (var consumer = new ConsumerBuilder<Null, string>(_configs).Build())
            {
                consumer.Subscribe(new string[] { _topicName });

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume();

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

                            if (consumeResult.Offset % commitPeriod == 0)
                            {
                                // The Commit method sends a "commit offsets" request to the Kafka
                                // cluster and synchronously waits for the response. This is very
                                // slow compared to the rate at which the consumer is capable of
                                // consuming messages. A high performance application will typically
                                // commit offsets relatively infrequently and be designed handle
                                // duplicate messages in the event of failure.
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }
    }
}
