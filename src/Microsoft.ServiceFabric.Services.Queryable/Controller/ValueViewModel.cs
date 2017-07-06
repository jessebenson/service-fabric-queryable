using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable.Controller
{
    public class ValueViewModel
    {
        public Guid PartitionId { get; set; }
        public JToken Key { get; set; }
        public JToken Value { get; set; }
   }
}
