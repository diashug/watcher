using System;
using Confluent.Kafka;

namespace KafkaLib
{
    public class ContentProducer
    {
        private string _topicName;
        private ProducerConfig _configs;

        public ContentProducer(string topicName)
        {
            _topicName = topicName;
            _configs = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
        }


        public async void SendMessage(string message)
        {
            using (var producer = new ProducerBuilder<Null, string>(_configs).Build())
            {
                try
                {
                    var deliveryResult = await producer.ProduceAsync(_topicName, new Message<Null, string> { Value = message });
                    Console.WriteLine($"delivered to: {deliveryResult.TopicPartitionOffset}");
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
            }
        }
    }
}
