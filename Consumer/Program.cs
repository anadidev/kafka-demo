using Confluent.Kafka;

using Consumer;

using System.Text.Json;

string topic = "test";
string groupId = "test_group";
string bootstrapServers = "localhost:9092";

var config = new ConsumerConfig
{
    GroupId = groupId,
    BootstrapServers = bootstrapServers,
    AutoOffsetReset = AutoOffsetReset.Earliest
};

try
{
    using (var consumerBuilder = new ConsumerBuilder
    <Ignore, string>(config).Build())
    {
        consumerBuilder.Subscribe(topic);
        var cancelToken = new CancellationTokenSource();

        try
        {
            while (true)
            {
                var consumer = consumerBuilder.Consume
                   (cancelToken.Token);
                var orderRequest = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);
                Console.WriteLine($"Processing Order: {consumer.Message.Value}");
            }
        }
        catch (OperationCanceledException)
        {
            consumerBuilder.Close();
        }
    }
}
catch (Exception ex)
{
    System.Diagnostics.Debug.WriteLine(ex.Message);
}
Console.ReadLine();