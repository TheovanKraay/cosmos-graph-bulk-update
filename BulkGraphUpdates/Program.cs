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
        static async Task Main(string[] args)
        {
            try
            {
                //We are going to update all the devices in partition "fleet1", of model "typeR", to have status = "on"
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
            List<VertexDevice> persons;
            if (this.faileddocs.Count != 0)
            {
                persons = new List<VertexDevice>();
                foreach (String docid in this.faileddocs)
                {
                    ItemResponse<VertexDevice> response = await container.ReadItemAsync<VertexDevice>(partitionKey: new PartitionKey("fleet1"), id: docid);
                    persons.Add(response);
                }
            }

            else
            {
                //get vertices where property called "model" = 'typeR'
                QueryDefinition query = new QueryDefinition("SELECT * FROM c where c.model[0]._value = 'typeR'");

                persons = new List<VertexDevice>();
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
                        persons.AddRange(response);
                    }
            }

            //re-set failed docs to null here to stop infinite recursion
            faileddocs = new List<String>();

            //test optimistic concurrency by updating a doc within a 30 second window to force IfMatchEtag to fail
            Console.WriteLine("waiting......");
            Thread.Sleep(30000);

            {
                //set up concurrentTasks for bulk upsert
                List<Task> concurrentTasks = new List<Task>();
                foreach (VertexDevice device in persons)
                {
                    //change property "status" to be a different value in each node
                    device.status[0]._value = "on";

                    //add tasks to concurrent tasks list
                    concurrentTasks.Add(Update(device));

                }
                //bulk update the graph objects in fleet1, changing status of all "typeR" models to "on"
                await Task.WhenAll(concurrentTasks);


                if (this.faileddocs.Count != 0)
                {
                    //recursive method call to re-apply change where replaceItem failed IfMatchEtag  
                    Console.WriteLine("Retrying docs where IfMatchEtag failed...");
                    await new Program().BulkUpdateGraphAsync();
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
        internal string Id { get; set; }

        [JsonProperty("_value")]
        internal string _value { get; set; }

    }
}
