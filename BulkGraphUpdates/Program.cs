namespace BulkGraphUpdates
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Newtonsoft.Json;

    class Program
    {
        //From Keys in portal...
        static string endpoint = ".NET SDK URI";
        static string authKey = "PRIMARY KEY";
        Container container = client.GetContainer("graphdb", "graph");

        //create cosmos client with bulk support enabled
        static CosmosClient client = new CosmosClient(endpoint, authKey, new CosmosClientOptions() { AllowBulkExecution = true });       

        List<String> faileddocs = new List<String>();
        List<VertexDevice> vertices;
        static async Task Main(string[] args)
        {
            try
            {
                //We are going to update all the devices in partition "fleet1", to increase temperature by 20
                Console.WriteLine("Bulk updating nodes in graph...");
                await new Program().BulkUpdateGraphAsync();
                Console.WriteLine("Update done!");
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e);
            }
        }

        private async Task BulkUpdateGraphAsync()
        {            
            
            //if we get failed docs, it means the optimistic concurrency control test failed due to another process updating the doc at the same time, and 
            //we want to check get the temperature to ensure we are increasing all device temperatures by the same amount
            if (this.faileddocs.Count != 0)
            {
                List<Task> concurrentTasks = new List<Task>();
                this.vertices = new List<VertexDevice>();
                foreach (String docid in this.faileddocs)
                {
                    //add tasks to concurrent tasks list
                    concurrentTasks.Add(Read(docid));               
                }
                //bulk read each failed doc, adding each to vertices to be re-processed.
                await Task.WhenAll(concurrentTasks);
            }

            else
            {
                //get vertices in 'fleet1' as we want to increase all of them by 20
                QueryDefinition query = new QueryDefinition("SELECT * FROM c where c.pk = 'fleet1'");

                vertices = new List<VertexDevice>();
                using (FeedIterator<VertexDevice> resultSet = container.GetItemQueryIterator<VertexDevice>(
                    queryDefinition: query,
                    requestOptions: new QueryRequestOptions()
                    {
                        //these devices have been modelled with partition key value of "fleet1"
                        PartitionKey = new PartitionKey("fleet1"),
                    }))
                    while (resultSet.HasMoreResults)
                    {
                        FeedResponse<VertexDevice> response = await resultSet.ReadNextAsync();
                        vertices.AddRange(response);
                    }
            }

            //re-set failed docs to null here to stop infinite recursion
            faileddocs = new List<String>();

            //test optimistic concurrency by updating a doc within a 30 second window to force IfMatchEtag to fail
            Console.WriteLine("waiting 30 seconds to simulate concurrent updates before applying bulk update......");
            Thread.Sleep(30000);

            {
                //set up concurrentTasks for bulk upsert
                List<Task> concurrentTasks = new List<Task>();
                foreach (VertexDevice device in vertices)
                {
                    //increase temperature setting by 20
                    device.temp[0]._value = device.temp[0]._value + 20;

                    //we put a safeguard here to ensure no updates that take temp above 140
                    if (device.temp[0]._value < 140)
                    {
                        //add tasks to concurrent tasks list
                        concurrentTasks.Add(Update(device));
                    }

                }
                //bulk update the graph objects in fleet1, increasing temperature of all devices by 20. 
                await Task.WhenAll(concurrentTasks);

                if (this.faileddocs.Count != 0)
                {
                    //recursive method call to re-apply change where replaceItem failed IfMatchEtag  
                    Console.WriteLine("Retrying docs where IfMatchEtag failed...");
                    await BulkUpdateGraphAsync();
                }
            }

        }

        private async Task Update(VertexDevice device)
        {
            try
            {
                //check IfMatchEtag to ensure that no changes have occured since initial read of the document
                await container.ReplaceItemAsync(device, device.Id, new PartitionKey("fleet1"), new ItemRequestOptions { IfMatchEtag = device._etag });
            }
            catch (CosmosException cre)
            {
                if (cre.StatusCode.ToString().Equals("PreconditionFailed"))
                {                    
                    this.faileddocs.Add(device.Id);
                }                
            }
        }

        private async Task Read(string docid)
        {
            ItemResponse<VertexDevice> response = await container.ReadItemAsync<VertexDevice>(partitionKey: new PartitionKey("fleet1"), id: docid);
            vertices.Add(response);
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
        public List<VertexProperty> model { get; set; }

        [JsonProperty("temp")]
        public List<VertexPropertyNumber> temp { get; set; }

        [JsonProperty("status")]
        public List<VertexProperty> status { get; set; }

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
    public class VertexProperty
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("_value")]
        public string _value { get; set; }

    }

    public class VertexPropertyNumber
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("_value")]
        public int _value { get; set; }

    }
}
