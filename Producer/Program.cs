using Confluent.Kafka;

using Producer;

using System.Net;
using System.Text.Json;

string bootstrapServers = "localhost:9092";
string topic = "test";

ProducerConfig config = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    ClientId = Dns.GetHostName()
};

try
{
    OrderRequest order = new OrderRequest
    {
        OrderId = 1,
        ProductId = 2,
        CustomerId = 3,
        Quantity = 10,
        Status = "Orderd"
    };
    using (var producer = new ProducerBuilder<Null, string>(config).Build())
    {
        var result = await producer.ProduceAsync(topic, new Message<Null, string>
        {
            Value = JsonSerializer.Serialize(order)
        });

        Console.WriteLine($"Delivery Timestamp: {result.Timestamp.UtcDateTime}");
        Console.WriteLine($"Message : {result.Value}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Error occured: {ex.Message}");
}

Console.ReadLine();

