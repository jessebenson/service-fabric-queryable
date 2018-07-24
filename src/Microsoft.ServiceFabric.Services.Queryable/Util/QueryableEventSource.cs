using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Text;

namespace Microsoft.ServiceFabric.Services.Queryable.Util
{
    [EventSource(Guid = "2757f245-65e9-4c32-8726-c4fe060cbfb1", Name = "SFQueryable")]
    internal class QueryableEventSource : EventSource
    {
        public static QueryableEventSource Log = new QueryableEventSource();

        [Event(1, Message = "{1}: {2}, errorCode: {3}", Level = EventLevel.Error)]
        public void ClientError(string traceId, string message, int errorCode)
        {
            WriteEvent(1, traceId, message, errorCode);
        }

        [Event(2, Message = "{1}: {2}, errorCode: {3}", Level = EventLevel.Error)]
        public void ServerError(string traceId, string message, int errorCode)
        {
            WriteEvent(2, traceId, message, errorCode);
        }

        [Event(3, Message = "{1}: {2}", Level = EventLevel.Informational)]
        public void Info(string traceId, string message)
        {
            WriteEvent(3, traceId, message);
        }
    }
}
