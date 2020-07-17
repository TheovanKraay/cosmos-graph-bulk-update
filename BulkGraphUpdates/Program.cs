using System;

namespace BulkGraphUpdates
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Newtonsoft.Json;

    class Program
    {
        static string endpoint = "";
        static string authKey = "";
        static CosmosClient client = new CosmosClient(endpoint, authKey, new CosmosClientOptions() { AllowBulkExecution = true });

        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            try {
                await RunDemoAsync();
            }
            catch(Exception e)
            {
                Console.WriteLine("Exception: " + e);
            }
        }

        private static async Task RunDemoAsync()
        {
            Container container = client.GetContainer("graphdb", "test");
            try
            {
                

                QueryDefinition query = new QueryDefinition("SELECT * FROM c");

                List<Person> persons = new List<Person>();
                using (FeedIterator<Person> resultSet = container.GetItemQueryIterator<Person>(
                    queryDefinition: query,
                    requestOptions: new QueryRequestOptions()
                    {
                        PartitionKey = new PartitionKey("test"),
                    }))
                {
                    while (resultSet.HasMoreResults)
                    {
                        try {
                            Thread.Sleep(1000);
                            FeedResponse<Person> response = await resultSet.ReadNextAsync();
                            Person person = response.First();
                            Thread.Sleep(1000);
                            persons.AddRange(response);
                        }
                        catch(Exception e)
                        {
                            Console.WriteLine("Exception: " + e);
                        }                                                
                    }

                    List<Task> concurrentTasks = new List<Task>();
                    foreach (Person person in persons)
                    {
                        Console.WriteLine("Id:" + person.Id);
                        Console.WriteLine("pk:" + person.pk);
                        Console.WriteLine("label:" + person.label);
                        person.label = "label2";
                        concurrentTasks.Add(container.UpsertItemAsync(person, new PartitionKey("test")));
                    }
                    await Task.WhenAll(concurrentTasks);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e);
            }





            //List<Person> persons = new List<Person>();

        }
    }

    public class Person
    {

        [JsonProperty("id")]
        internal string Id { get; set; }

        [JsonProperty("pk")]
        internal string pk { get; set; }

        [JsonProperty("label")]
        internal string label { get; set; }

        [JsonProperty("_etag")]
        internal string ETag { get; set; }

    }
}
