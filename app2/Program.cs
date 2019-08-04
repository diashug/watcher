using System;
using KafkaLib;

namespace App2
{
    class Program
    {
        private static string _consumerTopicName = "app1-outbox";
        private static string _producerTopicName = "app2-outbox";

        static void Main(string[] args)
        {
            Console.WriteLine("App2 initialised ...");

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
