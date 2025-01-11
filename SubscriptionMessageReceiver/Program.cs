using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

class Program
{
    private const string ServiceBusConnectionString = "Endpoint=sb://logisticsnamespace.servicebus.windows.net/;SharedAccessKeyName=shipmentupdatemanagedkey;SharedAccessKey=DTci1RzEBhNW/5LoL54h1sue0eBACDMPM+ASbJKrNRQ=;EntityPath=shipmentupdates";
    private const string TopicName = "shipmentupdates";

    static async Task Main(string[] args)
    {
        // Check if subscription name is provided
        if (args.Length != 1)
        {
            Console.WriteLine("Usage: dotnet run --<SubscriptionName>");
            return;
        }

        // Parse subscription name from arguments
        string subscriptionName = args[0].TrimStart('-');
        Console.WriteLine($"Receiving messages from subscription: {subscriptionName}");

        // Create Service Bus client
        ServiceBusClient client = new ServiceBusClient(ServiceBusConnectionString);
        ServiceBusProcessor processor = client.CreateProcessor(TopicName, subscriptionName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1 // Set to receive only one message at a time
        });

        try
        {
            // Register message and error handlers
            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;

            // Start receiving messages
            Console.WriteLine($"Listening to subscription: {subscriptionName}");
            await processor.StartProcessingAsync();

            Console.WriteLine("Press any key to stop receiving messages...");
            Console.ReadKey();

            // Stop receiving messages
            Console.WriteLine("Stopping receiver...");
            await processor.StopProcessingAsync();
        }
        finally
        {
            await processor.DisposeAsync();
            await client.DisposeAsync();
        }
    }

    private static async Task MessageHandler(ProcessMessageEventArgs args)
    {
        string body = args.Message.Body.ToString();
        Console.WriteLine($"Received message: {body}");

        // Simulate message processing delay
        await Task.Delay(10000);

        // Complete the message
        await args.CompleteMessageAsync(args.Message);
    }

    private static Task ErrorHandler(ProcessErrorEventArgs args)
    {
        Console.WriteLine($"Error occurred: {args.Exception.Message}");
        return Task.CompletedTask;
    }
}
