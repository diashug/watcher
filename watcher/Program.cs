using Sentry;
using System;

namespace Watcher
{
    class Program
    {
        static void Main(string[] args)
        {
            using (SentrySdk.Init("https://8c0cf70994fa459998cdcf3df5cf05d5@sentry.io/1509318"))
            {
                //Console.WriteLine(1 / 0);
            }
        }
    }
}
