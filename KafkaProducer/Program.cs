using RdKafka;
using System;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            string brokerList = "localhost:9092";
            string topicName = "test";

            using (Producer producer = new Producer(brokerList))
            {
                using (Topic topic = producer.Topic(topicName))
                {
                    Console.WriteLine("{" + producer.Name + "} producing on {" + topic.Name + "}. q to exit.");

                    string text;
                    while ((text = Console.ReadLine()) != "q")
                    {
                        byte[] data = Encoding.UTF8.GetBytes(text);
                        Task<DeliveryReport> deliveryReport = topic.Produce(data);
                        var unused = deliveryReport.ContinueWith(task =>
                        {
                            Console.WriteLine("Partition: {" + task.Result.Partition + "}, Offset: {" + task.Result.Offset + "}");
                        });
                    }
                }
            }
        }
    }
}
