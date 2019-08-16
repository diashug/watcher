using System;
using KafkaLib;

namespace App1
{
    class Program
    {
        private static string _consumerTopicName = "app1-inbox";
        private static string _producerTopicName = "app1-outbox";
        private static string _checkpointTopicName = "app1-checkpoint";

        private static ContentConsumer cc = new ContentConsumer(_consumerTopicName);
        private static ContentProducer cp = new ContentProducer(_producerTopicName);
        private static ContentProducer cp1 = new ContentProducer(_checkpointTopicName);

        static void Main(string[] args)
        {
            Console.WriteLine("App1 initialised ...");

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
