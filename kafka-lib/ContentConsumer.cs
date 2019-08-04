using Confluent.Kafka;
using System;
using System.Threading;

namespace KafkaLib
{
    public delegate void ConsumerEventHandler(object sender, ConsumerEventArgs e);

    public class ContentConsumer
    {
        private string _topicName;
        private ConsumerConfig _configs;

        public event ConsumerEventHandler OnMessageReceived;

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

        public void MessageReceived(ConsumerEventArgs e)
        {
            OnMessageReceived?.Invoke(this, e);
        }

        public void ConsumeMessages()
        {
            /*using (SentrySdk.Init("https://8c0cf70994fa459998cdcf3df5cf05d5@sentry.io/1509318"))
            {

            }*/

            var args = new ConsumerEventArgs();

            using (var consumer = new ConsumerBuilder<Null, string>(_configs).Build())
            {
                consumer.Subscribe(new string[] { _topicName });

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            args.message = consumeResult.Value;
                            MessageReceived(args);

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

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
