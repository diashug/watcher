using KafkaLib;
using MongoDB.Bson;
using MongoDB.Driver;
using Sentry;
using System;
using System.Collections.Generic;
using System.Timers;
using Watcher.Models;

namespace Watcher
{
    class Program
    {
        private static long globalSLA = 10000;

        private static MongoClient mongoClient = new MongoClient("mongodb://localhost:27017");
        private static IMongoCollection<TestModel> collection = mongoClient.GetDatabase("watcher").GetCollection<TestModel>("tests");

        private static ContentProducer cp = new ContentProducer("app1-inbox");
        private static ContentConsumer cc1 = new ContentConsumer("app1-checkpoint");
        private static ContentConsumer cc2 = new ContentConsumer("last-checkpoint");
        private static TestModel tm;

        static void Main(string[] args)
        {
            Console.WriteLine("Watcher initialised ...");
            Console.WriteLine("Available options:");
            Console.WriteLine("1 - Send Test Message");
            Console.WriteLine("2 - Send Normal Message");

            cc1.OnMessageReceived += OnMessageReceivedCheckpoint1;
            cc2.OnMessageReceived += OnMessageReceivedLastCheckpoint;

            while (true)
            {
                Console.WriteLine("Enter option:");
                var option = Console.ReadLine();

                if (option == "1")
                {
                    var tm = new TestModel
                    {
                        Id = Guid.NewGuid().ToString(),
                        TestName = "test_" + DateTimeOffset.Now.ToUnixTimeMilliseconds().ToString(),
                        CreationDate = new DateTime()
                    };

                    collection.InsertOne(tm);

                    Console.WriteLine("Sending test message ...");
                    cp.SendMessage(tm.Id + "|Test Message");

                } else if (option == "2")
                {
                    Console.WriteLine("Sending normal message ...");
                    cp.SendMessage("NORMAL|Normal Message");
                } else
                {
                    Console.WriteLine("Option not available.");
                }
            }
        }

        static void OnMessageReceivedCheckpoint1(object sender, ConsumerEventArgs e)
        {
            var message = e.message.Split("|");
            
            if (message[0] == tm.Id)
            {
                Console.WriteLine(@"Test message received.");

                var filters = new Dictionary<string, string>();
                filters.Add("Id", tm.Id);

                var filter = new BsonDocument(filters);
                var update = new BsonDocument("$set", new BsonDocument("checkpoint1Timestamp", DateTimeOffset.Now.ToUnixTimeMilliseconds()));

                collection.UpdateOne(filter, update);
            }
        }

        static void OnMessageReceivedLastCheckpoint(object sender, ConsumerEventArgs e)
        {
            var message = e.message.Split("|");


        }
    }
}
