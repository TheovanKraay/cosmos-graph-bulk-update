using System;

namespace BulkGraphUpdates
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Newtonsoft.Json;

    class Program
    {
        //From Keys in portal...
        static string endpoint = ".NET SDK URI";
        static string authKey = "PRIMARY KEY";

        //create cosmos client with bulk support enabled
        static CosmosClient client = new CosmosClient(endpoint, authKey, new CosmosClientOptions() { AllowBulkExecution = true });

        static async Task Main(string[] args)
        {
            try
            {
                //We are going to update all the devices in partition "fleet1", of model "typeR", to have status = "on"
                Console.WriteLine("Bulk updating nodes in graph...");
                await BulkUpdateGraphAsync();
                Console.WriteLine("Update done!");
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e);
            }
        }

        private static async Task BulkUpdateGraphAsync()
        {
            Container container = client.GetContainer("graphdb", "graph");
            try
            {

                //get vertices where property called "model" = 'typeR'
                QueryDefinition query = new QueryDefinition("SELECT * FROM c where c.model[0]._value = 'typeR'");

                List<VertexDevice> persons = new List<VertexDevice>();
                using (FeedIterator<VertexDevice> resultSet = container.GetItemQueryIterator<VertexDevice>(
                    queryDefinition: query,
                    requestOptions: new QueryRequestOptions()
                    {
                        //these devices have been modelled with partition key value of "fleet1"
                        PartitionKey = new PartitionKey("fleet1"),
                    }))
                {
                    while (resultSet.HasMoreResults)
                    {
                        try
                        {
                            FeedResponse<VertexDevice> response = await resultSet.ReadNextAsync();
                            VertexDevice person = response.First();
                            persons.AddRange(response);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception: " + e);
                        }
                    }

                    //set up concurrentTasks for bulk upsert
                    List<Task> concurrentTasks = new List<Task>();
                    foreach (VertexDevice device in persons)
                    {
                        //change property "name" to be a different value in each node
                        device.status[0]._value = "on";
                        //devices have been modelled with partition key value of "fleet1"
                        concurrentTasks.Add(container.UpsertItemAsync(device, new PartitionKey("fleet1")));
                    }

                    //bulk update the graph objects in fleet1, changing status of all "typeR" models to "on"
                    await Task.WhenAll(concurrentTasks);
                }                
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e);
            }
        }
    }

    public class VertexDevice
    {

        [JsonProperty("id")]
        internal string Id { get; set; }

        [JsonProperty("pk")]
        internal string pk { get; set; }

        [JsonProperty("label")]
        internal string label { get; set; }

        [JsonProperty("model")]
        public List<DeviceModel> model { get; set; }

        [JsonProperty("status")]
        public List<DeviceStatus> status { get; set; }

        [JsonProperty("_rid")]
        internal string _rid { get; set; }

        [JsonProperty("_self")]
        internal string _self { get; set; }

        [JsonProperty("_etag")]
        internal string _etag { get; set; }

        [JsonProperty("_attachments")]
        internal string _attachments { get; set; }

        [JsonProperty("_ts")]
        internal string _ts { get; set; }

    }
    public class DeviceModel
    {
        [JsonProperty("id")]
        internal string Id { get; set; }

        [JsonProperty("_value")]
        internal string _value { get; set; }

    }

    public class DeviceStatus
    {
        [JsonProperty("id")]
        internal string Id { get; set; }

        [JsonProperty("_value")]
        internal string _value { get; set; }

    }
}
