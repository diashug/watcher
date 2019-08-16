using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Watcher.Models
{
    class TestModel
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonElement("testName")]
        public string TestName { get; set; }

        [BsonElement("checkpoint1Timestamp")]
        public long Checkpoint1Timestamp { get; set; }

        [BsonElement("lastCheckpointTimestamp")]
        public long LastCheckpointTimestamp { get; set; }

        [BsonElement("creationDate")]
        public DateTime CreationDate { get; set; }
    }
}
