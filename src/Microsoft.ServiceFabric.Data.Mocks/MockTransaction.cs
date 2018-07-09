using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Data.Mocks
{
	public class MockTransaction : ITransaction
	{
		private static long _sequenceNumber = 0;

		public long CommitSequenceNumber { get; private set; } = -1;

		public long TransactionId { get; } = Interlocked.Increment(ref _sequenceNumber);

		void ITransaction.Abort()
		{
		}

		Task ITransaction.CommitAsync()
		{
			CommitSequenceNumber = Interlocked.Increment(ref _sequenceNumber);
			return Task.CompletedTask;
		}

		void IDisposable.Dispose()
		{
		}

		Task<long> ITransaction.GetVisibilitySequenceNumberAsync()
		{
			throw new NotImplementedException();
		}
	}
}
