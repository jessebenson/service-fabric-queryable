using Newtonsoft.Json.Linq;
using System;


namespace Microsoft.ServiceFabric.Services.Queryable.Controller
{
    public class ValueViewModel
    {
        public Guid PartitionId { get; set; }
        public JToken Key { get; set; }
        public JToken Value { get; set; }
   }
}
