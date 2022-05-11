using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using Confluent.SchemaRegistry;
using Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;

internal class Consumer
{
    static void Main(string[] args)
    {
        //--- Data Topic ---//
        const string topic = "house";

        //--- Temporapry stopping the incoming data stream ---//
        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {//The key is depending on the environments standards.
            e.Cancel = true; 
            cts.Cancel();
        };

        var config = new ConsumerConfig
        {
            GroupId = Guid.NewGuid().ToString(),
            BootstrapServers = "172.16.250.13:9092",
            //AutoOffsetReset = AutoOffsetReset.Latest //Get only new data after startup.
            AutoOffsetReset = AutoOffsetReset.Earliest //Give me all the data you have, also with regression.
        };

        using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = "172.16.250.12:8081" }))
        using (var consumer = new ConsumerBuilder<Null, House>(config).SetValueDeserializer(new AvroDeserializer<House>(schemaRegistry).AsSyncOverAsync()).Build())
        {
            double MinEl = double.MaxValue; //
            double MinElLoca = double.MaxValue; //
            double MaxElLoca = 0; //
            double TotalElLoca = 0; //
            double AvgElLoca = 0; //
            string iterDate = "";
            Dictionary<String, List<double>> DictElLoca = new Dictionary<string, List<double>>();

            consumer.Subscribe(topic); //Get data from this topic. 
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token); //Parameter is for freesing program.
                    //--- GET RAW DATA ---//
                    string tempDate = cr.Message.Value.Reading.ToString("yyyy-MM-dd");
                    if (tempDate != iterDate)
                    {
                        Dictionary<string, double> dictMinElLoca = new Dictionary<string, double>();
                        Dictionary<string, double> dictMaxElLoca = new Dictionary<string, double>();
                        Dictionary<string, double> dictAvgElLoca = new Dictionary<string, double>();
                        foreach(KeyValuePair<String, List<double>> kv in DictElLoca)
                        {   //--- FILTER && OPERATE ON RAW DATA ---//
                            foreach(var inEl in kv.Value)
                            {
                                //--- min electricity usage ---//
                                MinElLoca = Math.Min(MinEl, cr.Message.Value.ElectricityUsage); //Get absolute minimum value of ElectricityUsage.
                                dictMinElLoca[cr.Message.Value.Location] = MinElLoca;
                                
                                //--- max electricity usage ---//
                                MaxElLoca = Math.Max(MaxElLoca, cr.Message.Value.ElectricityUsage); //Get absolute minimum value of ElectricityUsage.
                                dictMaxElLoca[cr.Message.Value.Location] = MaxElLoca;

                                //--- average electricity usage ---//
                                TotalElLoca += cr.Message.Value.ElectricityUsage; //Get absolute minimum value of ElectricityUsage.
                            }
                            AvgElLoca = TotalElLoca / DictElLoca.Count;
                            dictAvgElLoca[cr.Message.Value.Location] = AvgElLoca;

                            Console.WriteLine($"{kv.Key}");
                            Console.WriteLine($"Min:{MinElLoca}");
                            Console.WriteLine($"Max:{MaxElLoca}");
                            Console.WriteLine($"Avg:{AvgElLoca}");
                            Thread.Sleep(1000);
                        }

                        DictElLoca = new Dictionary<string, List<double>>();
                        iterDate = tempDate;
                    }
                    if (DictElLoca.ContainsKey(cr.Message.Value.Location) == false)
                    {
                        DictElLoca[cr.Message.Value.Location] = new List<double>();
                    }
                    DictElLoca[cr.Message.Value.Location].Add(cr.Message.Value.ElectricityUsage);

                    Console.WriteLine(cr.Message.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss") + $":" + Environment.NewLine +
                        $" Location {cr.Message.Value.Location}, " + Environment.NewLine +
                        $" Electricity: {cr.Message.Value.ElectricityUsage}, " + Environment.NewLine +
                        $" Heating: {cr.Message.Value.HeatingUsage}, " + Environment.NewLine +
                        $" Water: {cr.Message.Value.WaterUsage}" + Environment.NewLine +
                        $" Reading Time: {cr.Message.Value.Reading}");

                    //--- Slows console window down for readability ---//
                    Thread.Sleep(20); //Vent lidt, s� vi kan f�lge med i dataene. 

                    //--- FILTER && OPERATE ON RAW DATA ---//
                    //--- min electricity usage ---//
                    MinEl = Math.Min(MinEl, cr.Message.Value.ElectricityUsage); //Get absolute minimum value of ElectricityUsage.
                    Console.Write($"\nMin. value of electricity usage: {MinEl}.\n \n");

                }//END while gettingData
            }//END try
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed. Which is CancellationToken.
            }
            finally
            {
                consumer.Close();
            }
        }//END using connection
    }//END main program
}//END claass


// using (var consumer = new ConsumerBuilder<string, string>(
//     configuration.AsEnumerable()).Build())
// {
//     consumer.Subscribe(topic);
//     try {
//         while (true) {
//             var cr = consumer.Consume(cts.Token);
//             Console.WriteLine($"Consumed event from topic {topic} with ke{cr.Message.Key,-10} and value {cr.Message.Value}");
//         }
//     }


// RUN: dotnet run --project consumer/consumer.csproj $(pwd)/getting-started.properties