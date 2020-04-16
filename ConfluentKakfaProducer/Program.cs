using Confluent.Kafka;
using System;

namespace ConfluentKakfaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                try
                {
                    string text;
                    while ((text = Console.ReadLine()) != "q")
                    {
                        var dr = p.ProduceAsync("mytopic", new Message<string, string> {Key="Mykey", Value = text }).ConfigureAwait(false).GetAwaiter().GetResult();
                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

            Console.ReadKey();
        }
    }
}
