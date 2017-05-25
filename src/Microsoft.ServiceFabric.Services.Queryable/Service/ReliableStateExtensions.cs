using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class ReliableStateExtensions
	{
		public static async Task<IEnumerable<KeyValuePair<string, Type>>> GetQueryableTypes(this IReliableStateManager stateManager, CancellationToken cancellationToken = default(CancellationToken))
		{
			var types = new Dictionary<string, Type>();
			var enumerator = stateManager.GetAsyncEnumerator();
			while (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
			{
				var state = enumerator.Current;
				if (state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				{
					types.Add(state.Name.AbsolutePath, state.GetValueType());
				}
			}

			return types;
		}
	}
}
