using System.Net;

namespace ServiceFabric.Extensions.Services.Queryable
{
	internal static class HttpStatusCodeExtensions
	{
		public static bool IsSuccessStatusCode(this HttpStatusCode status)
		{
			return 200 <= (int)status && (int)status < 300;
		}
	}
}
