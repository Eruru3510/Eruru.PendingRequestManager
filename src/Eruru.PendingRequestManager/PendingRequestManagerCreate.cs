namespace Eruru.PendingRequestManager {

#pragma warning disable IDE0079 // 请删除不必要的忽略
#pragma warning disable CA1815 // 重写值类型上的 Equals 和相等运算符
	readonly struct PendingRequestManagerCreate<TKey, TValue> (
#pragma warning restore CA1815 // 重写值类型上的 Equals 和相等运算符
#pragma warning restore IDE0079 // 请删除不必要的忽略
		PendingRequestManager<TKey, TValue>? pendingRequestManager, TKey key
	) : IDisposable where TKey : notnull {

		readonly PendingRequestManager<TKey, TValue>? PendingRequestManager = pendingRequestManager;
		readonly TKey Key = key;

		public void Dispose () {
			PendingRequestManager?.TrySetCanceled (Key);
		}

	}

}