using System.Collections.Concurrent;

namespace Eruru.PendingRequestManager {

#pragma warning disable CA1724 // 类型名与命名空间名称整体或部分冲突
	public class PendingRequestManager<TKey, TValue> : IDisposable where TKey : notnull {
#pragma warning restore CA1724 // 类型名与命名空间名称整体或部分冲突

		public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds (60);
		public int Count => PendingRequests.Count;

		readonly ConcurrentDictionary<TKey, PendingRequest> PendingRequests = [];
		int State;

		protected virtual void Dispose (bool disposing) {
			if (Interlocked.Exchange (ref State, 1) != 0 || !disposing) {
				return;
			}
			foreach (var pendingRequest in PendingRequests.ToArray ()) { // HACK: 改为等待所有 PendingRequest 执行完毕安全退出
				PendingRequests.TryRemove (pendingRequest.Key, out _);
				pendingRequest.Value.Dispose ();
			}
		}

		public void Dispose () {
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		public bool TryCreate (
			TKey key, out Task<TValue>? task
			, object? state = null, TaskCreationOptions taskCreationOptions = TaskCreationOptions.RunContinuationsAsynchronously
			, CancellationToken? cancellationToken = null
		) {
			CheckDisposed ();
			var taskCompletionSource = new TaskCompletionSource<TValue> (state, taskCreationOptions);
			CancellationTokenSource? cancellationTokenSource = null;
			PendingRequest pendingRequest;
			CancellationToken token;
			if (cancellationToken.HasValue) {
				token = cancellationToken.Value;
			} else {
#pragma warning disable CA2000 // 丢失范围之前释放对象
				cancellationTokenSource = new CancellationTokenSource (Timeout);
#pragma warning restore CA2000 // 丢失范围之前释放对象
				token = cancellationTokenSource.Token;
			}
			pendingRequest = new (taskCompletionSource, cancellationTokenSource, token);
			try {
				if (!PendingRequests.TryAdd (key, pendingRequest)) {
					pendingRequest.Dispose ();
					task = null;
					return false;
				}
			} catch {
				pendingRequest.Dispose ();
				throw;
			}
			try {
				if (token.CanBeCanceled) {
					var cancellationTokenRegistration = token.Register (static tokenState => {
						if (tokenState is not RegisterArgs registerArgs) {
							return;
						}
						registerArgs.TrySetCanceled ();
					}, new RegisterArgs (this, key), false);
					pendingRequest.CancellationTokenRegistration = cancellationTokenRegistration;
					if (token.IsCancellationRequested) {
						pendingRequest.TrySetCanceled ();
						task = taskCompletionSource.Task;
						return true;
					}
				}
			} catch {
				PendingRequests.TryRemove (key, out _);
				pendingRequest.Dispose ();
				throw;
			}
			if (Volatile.Read (ref State) != 0) {
				PendingRequests.TryRemove (key, out _);
				pendingRequest.Dispose ();
				throw new ObjectDisposedException (nameof (PendingRequestManager<,>));
			}
			task = taskCompletionSource.Task;
			return true;
		}

		public bool TrySetResult (TKey key, TValue value) {
			var isRemoved = PendingRequests.TryRemove (key, out var pendingRequest);
			try {
				if (isRemoved) {
					isRemoved = pendingRequest?.TrySetResult (value) ?? false;
				}
				if (!isRemoved) {
					CheckDisposed ();
				}
				return isRemoved;
			} finally {
				pendingRequest?.Dispose ();
			}
		}

		public bool TrySetException (TKey key, Exception exception) {
			var isRemoved = PendingRequests.TryRemove (key, out var pendingRequest);
			try {
				if (isRemoved) {
					isRemoved = pendingRequest?.TrySetException (exception) ?? false;
				}
				if (!isRemoved) {
					CheckDisposed ();
				}
				return isRemoved;
			} finally {
				pendingRequest?.Dispose ();
			}
		}

		public bool TrySetCanceled (TKey key) {
			var isRemoved = PendingRequests.TryRemove (key, out var pendingRequest);
			try {
				if (isRemoved) {
					isRemoved = pendingRequest?.TrySetCanceled () ?? false;
				}
				if (!isRemoved) {
					CheckDisposed ();
				}
				return isRemoved;
			} finally {
				pendingRequest?.Dispose ();
			}
		}

		void CheckDisposed () {
			if (Volatile.Read (ref State) == 0) {
				return;
			}
			throw new ObjectDisposedException (nameof (PendingRequestManager<,>));
		}

		readonly struct RegisterArgs (PendingRequestManager<TKey, TValue> pendingRequestManager, TKey key) {

			readonly PendingRequestManager<TKey, TValue> PendingRequestManager = pendingRequestManager;
			readonly TKey Key = key;

			public bool TrySetCanceled () {
				return PendingRequestManager.TrySetCanceled (Key);
			}

		}

		sealed class PendingRequest (
			TaskCompletionSource<TValue> taskCompletionSource, CancellationTokenSource? cancellationTokenSource
			, CancellationToken cancellationToken
		) : IDisposable {

			public CancellationTokenRegistration? CancellationTokenRegistration { get; set; }

			readonly TaskCompletionSource<TValue> TaskCompletionSource = taskCompletionSource;
			readonly CancellationTokenSource? CancellationTokenSource = cancellationTokenSource;
			readonly CancellationToken CancellationToken = cancellationToken;
			int State;

			public void Dispose () {
				if (Interlocked.Exchange (ref State, 1) != 0) {
					return;
				}
				CancellationTokenRegistration?.Dispose ();
				CancellationTokenSource?.Dispose ();
				TaskCompletionSource.TrySetException (new ObjectDisposedException (nameof (PendingRequestManager<,>)));
			}

			public bool TrySetResult (TValue value) {
				return TaskCompletionSource.TrySetResult (value);
			}

			public bool TrySetException (Exception exception) {
				return TaskCompletionSource.TrySetException (exception);
			}

			public bool TrySetCanceled () {
				return TaskCompletionSource.TrySetCanceled (CancellationToken);
			}

		}

	}

}