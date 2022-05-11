using Avro;
using Avro.Specific;
using System;
using System.IO;

namespace Kafka
{

    internal class House : ISpecificRecord
    {
        public static Schema _SCHEMA = Schema.Parse(File.ReadAllText("House.avsc"));
        public string Location { get; set; }
        public double WaterUsage { get; set; }
        public double ElectricityUsage { get; set; }
        public double HeatingUsage { get; set; }
        public DateTime Reading { get; set; }

        public Schema Schema => _SCHEMA;

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return Location;
                case 1: return WaterUsage;
                case 2: return ElectricityUsage;
                case 3: return HeatingUsage;
                case 4: return Reading;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: Location = (string)fieldValue; break;
                case 1: WaterUsage = (double)fieldValue; break;
                case 2: ElectricityUsage = (double)fieldValue; break;
                case 3: HeatingUsage = (double)fieldValue; break;
                case 4: Reading = new DateTime((long)fieldValue); break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            }
        }
    }
}