using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Messaging.ServiceBus;

// Key Vault
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;



namespace az_service_bus
{
    class Program
    {
        //static string connectionString = "Endpoint=sb://messagehdezapp.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mlgffZVGAp0qHg+oTcbeXcFLlrShgTmMP8ciz2vRRCo=";
        static private string queueName;
        static private string keyVaultName;
        static private string SecretName; 
        static private string ConnectionString;

        static async Task Main(string[] args)
        {
            using IHost host = CreateHostBuilder(args).Build();

            // Retrieve Secret from Key Vault: Service Bus Connection String
            var kvUri = "https://" + keyVaultName + ".vault.azure.net";

            var client = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());

            var secret = await client.GetSecretAsync(SecretName);

            ConnectionString = secret.Value.Value;

            // Console Menu
            var showMenu = true;
            while(showMenu)
            {
                Console.Clear();
                Console.WriteLine("Choose an option:");
                Console.WriteLine("1) Send a Message to the queue");
                Console.WriteLine("2) Send a Batch of Messages to the queue");
                Console.WriteLine("3) Retrieve Messages from the Queue");
                Console.WriteLine("4) Quit App");
                Console.Write("\r\nSelect an option: ");
            
                switch (Console.ReadLine())
                {
                    case "1":
                        Console.WriteLine("Sending a message to the Sales Messages queue...");
                        // send a message to the queue
                        await SendMessageAsync();
                        Console.WriteLine("Message was sent successfully.");
                        Console.WriteLine("Press Any Key to Continue...");
                        Console.ReadKey();
                        break;
                    case "2":
                        Console.WriteLine("Sending a batch of messages to the Sales Messages queue...");
                        // send a batch of messages to the queue
                        await SendMessageBatchAsync();
                        Console.WriteLine("Messages were sent successfully.");
                        Console.WriteLine("Press Any Key to Continue...");
                        Console.ReadKey();
                        break;
                    case "3":
                        Console.WriteLine("Receiving a message from the queue");
                        // receive message from the queue
                        await ReceiveMessagesAsync();
                        Console.WriteLine("Press Any Key to Continue...");
                        Console.ReadKey();
                        break;
                    case "4":          
                        showMenu = false;
                        break;
                    default:
                        Console.WriteLine("Please select a valid option");
                        Console.WriteLine("Press Any Key to Continue...");
                        Console.ReadKey();
                        break;
                }          
            }
            
        }
        static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((hostingContext, configuration) =>
            {
                configuration.Sources.Clear();

                IHostEnvironment env = hostingContext.HostingEnvironment;

                configuration
                    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                    .AddJsonFile($"appsettings.{env.EnvironmentName}.json", true, true);

                IConfigurationRoot configurationRoot = configuration.Build();
                
                Settings options = new();
                configurationRoot.GetSection(nameof(Settings))
                                    .Bind(options);

                //Console.WriteLine($"Settings.key_vault_name={options.key_vault_name}");
                //Console.WriteLine($"Settings.queue_name={options.queue_name}");
                //Console.WriteLine($"Settings.secret_name={options.secret_name}");

                // Save appsettings parameters in the static variables - is there a better way?
                keyVaultName = options.key_vault_name;
                queueName = options.queue_name;
                SecretName = options.secret_name;


            ;}); 

        public class Settings{
            public string key_vault_name {get; set;}
            public string queue_name {get; set;}
            public string secret_name {get; set;}
        }

        // Send Messages Block
        static async Task SendMessageAsync()
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(ConnectionString))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(queueName);

                string message_content = $"$10,000 order for bicycle parts from retailer Adventure Works.";
                // create a message that we can send
                ServiceBusMessage message = new ServiceBusMessage(message_content);

                Console.WriteLine($"Sending message: {message_content}");

                // send the message
                await sender.SendMessageAsync(message);
                Console.WriteLine($"Sent a single message to the queue: {queueName}");
            }
        }

        // Typically, you get these messages from different parts of your application. Here, we create a queue of sample messages.
        static Queue<ServiceBusMessage> CreateMessages()
        {
            // create a queue containing the messages and return it to the caller
            Queue<ServiceBusMessage> messages = new Queue<ServiceBusMessage>();
            messages.Enqueue(new ServiceBusMessage("First message in the batch"));
            messages.Enqueue(new ServiceBusMessage("Second message in the batch"));
            messages.Enqueue(new ServiceBusMessage("Third message in the batch"));
            return messages;
        }

        static async Task SendMessageBatchAsync()
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(ConnectionString))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(queueName);

                // get the messages to be sent to the Service Bus queue
                Queue<ServiceBusMessage> messages = CreateMessages();

                // total number of messages to be sent to the Service Bus queue
                int messageCount = messages.Count;

                // while all messages are not sent to the Service Bus queue
                while (messages.Count > 0)
                {
                    // start a new batch 
                    using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                    // add the first message to the batch
                    if (messageBatch.TryAddMessage(messages.Peek()))
                    {
                        // dequeue the message from the .NET queue once the message is added to the batch
                        messages.Dequeue();
                    }
                    else
                    {
                        // if the first message can't fit, then it is too large for the batch
                        throw new Exception($"Message {messageCount - messages.Count} is too large and cannot be sent.");
                    }

                    // add as many messages as possible to the current batch
                    while (messages.Count > 0 && messageBatch.TryAddMessage(messages.Peek()))
                    {
                        // dequeue the message from the .NET queue as it has been added to the batch
                        messages.Dequeue();
                    }

                    // now, send the batch
                    await sender.SendMessagesAsync(messageBatch);

                    // if there are any remaining messages in the .NET queue, the while loop repeats 
                }

                Console.WriteLine($"Sent a batch of {messageCount} messages to the topic: {queueName}");
            }
        }
        // handle received messages
        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            // complete the message. messages is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        static async Task ReceiveMessagesAsync()
        {
            await using (ServiceBusClient client = new ServiceBusClient(ConnectionString))
            {
                // create a processor that we can use to process the messages
                ServiceBusProcessor processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());

                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

                Console.WriteLine("Wait for a minute and then press any key to end the processing");
                Console.ReadKey();

                // stop processing 
                Console.WriteLine("\nStopping the receiver...");
                await processor.StopProcessingAsync();
                Console.WriteLine("Stopped receiving messages");
            }
        }
    }
}
