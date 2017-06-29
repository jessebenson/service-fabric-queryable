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
        public JObject Key { get; set; }
        public JObject Value { get; set; }
   }
}
