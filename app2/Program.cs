using System;
using KafkaLib;

namespace App2
{
    class Program
    {
        private static string _consumerTopicName = "app1-outbox";
        private static string _producerTopicName = "app2-outbox";
        private static string _checkpointTopicName = "last-checkpoint";

        private static ContentConsumer cc = new ContentConsumer(_consumerTopicName);
        private static ContentProducer cp = new ContentProducer(_producerTopicName);
        private static ContentProducer cp1 = new ContentProducer(_checkpointTopicName);

        static void Main(string[] args)
        {
            Console.WriteLine("App2 initialised ...");

            cc.OnMessageReceived += OnMessageReceived;

            Console.WriteLine("Message handler started ...");

            cc.ConsumeMessages();
        }

        static void OnMessageReceived(object sender, ConsumerEventArgs e)
        {
            cp.SendMessage(e.message);
            cp1.SendMessage(e.message);

            Console.WriteLine($"{sender.ToString()} sent {e.message}");
        }
    }
}
