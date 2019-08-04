using System;
using KafkaLib;

namespace App1
{
    class Program
    {
        private static string _consumerTopicName = "app1-inbox";
        private static string _producerTopicName = "app1-outbox";

        static void Main(string[] args)
        {
            Console.WriteLine("App1 initialised ...");

            var cc = new ContentConsumer(_consumerTopicName);

            cc.OnMessageReceived += HandleMessageReceived;

            Console.WriteLine("Message handler started ...");

            cc.ConsumeMessages();
        }

        static void HandleMessageReceived(object sender, ConsumerEventArgs e)
        {
            var cp = new ContentProducer(_producerTopicName);

            cp.SendMessage(e.message + " " + Guid.NewGuid() + " " + new DateTime().Ticks.ToString());
            Console.WriteLine($"{sender.ToString()} sent {e.message}");
        }
    }
}
