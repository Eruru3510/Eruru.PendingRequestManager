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
			TKey key, out Task<TValue>? task, object? state, TaskCreationOptions taskCreationOptions,
			CancellationToken cancellationToken
		) {
			CheckDisposed ();
#pragma warning disable CA2000 // 丢失范围之前释放对象
			var cancellationTokenSource = new CancellationTokenSource (Timeout);
			var cancellationTokenSource1 = CancellationTokenSource.CreateLinkedTokenSource (
#pragma warning restore CA2000 // 丢失范围之前释放对象
				cancellationTokenSource.Token, cancellationToken
			);
			cancellationToken = cancellationTokenSource1.Token;
			var taskCompletionSource = new TaskCompletionSource<TValue> (state, taskCreationOptions);
			var pendingRequest = new PendingRequest (
				taskCompletionSource, cancellationTokenSource, cancellationTokenSource1, cancellationToken
			);
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
				if (cancellationToken.CanBeCanceled) {
					var cancellationTokenRegistration = cancellationToken.Register (static state => {
						if (state is not ValueTuple<PendingRequestManager<TKey, TValue>, TKey> tuple) {
							return;
						}
						tuple.Item1.TrySetCanceled (tuple.Item2);
					}, (this, key), false);
					pendingRequest.CancellationTokenRegistration = cancellationTokenRegistration;
					if (cancellationToken.IsCancellationRequested) {
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
		public bool TryCreate (TKey key, out Task<TValue>? task, object? state, CancellationToken cancellationToken) {
			return TryCreate (key, out task, state, TaskCreationOptions.RunContinuationsAsynchronously, cancellationToken);
		}
		public bool TryCreate (
			TKey key, out Task<TValue>? task, TaskCreationOptions taskCreationOptions,
			CancellationToken cancellationToken
		) {
			return TryCreate (key, out task, null, taskCreationOptions, cancellationToken);
		}
		public bool TryCreate (TKey key, out Task<TValue>? task, CancellationToken cancellationToken) {
			return TryCreate (key, out task, null, TaskCreationOptions.RunContinuationsAsynchronously, cancellationToken);
		}
		public bool TryCreate (
			TKey key, out Task<TValue>? task, object? state = null,
			TaskCreationOptions taskCreationOptions = TaskCreationOptions.RunContinuationsAsynchronously
		) {
			return TryCreate (key, out task, state, taskCreationOptions, CancellationToken.None);
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

		public IDisposable Create (
			TKey key, out Task<TValue>? task, object? state, TaskCreationOptions taskCreationOptions,
			CancellationToken cancellationToken
		) {
			if (!TryCreate (key, out task, state, taskCreationOptions, cancellationToken)) {
				return new PendingRequestManagerCreate (null, key);
			}
			return new PendingRequestManagerCreate (this, key);
		}
		public IDisposable Create (TKey key, out Task<TValue>? task, object? state, CancellationToken cancellationToken) {
			return Create (key, out task, state, TaskCreationOptions.RunContinuationsAsynchronously, cancellationToken);
		}
		public IDisposable Create (
			TKey key, out Task<TValue>? task, TaskCreationOptions taskCreationOptions, CancellationToken cancellationToken
		) {
			return Create (key, out task, null, taskCreationOptions, cancellationToken);
		}
		public IDisposable Create (
			TKey key, out Task<TValue>? task, CancellationToken cancellationToken
		) {
			return Create (key, out task, null, TaskCreationOptions.RunContinuationsAsynchronously, cancellationToken);
		}
		public IDisposable Create (
			TKey key, out Task<TValue>? task, object? state = null,
			TaskCreationOptions taskCreationOptions = TaskCreationOptions.RunContinuationsAsynchronously
		) {
			return Create (key, out task, state, taskCreationOptions, CancellationToken.None);
		}

		void CheckDisposed () {
			if (Volatile.Read (ref State) == 0) {
				return;
			}
			throw new ObjectDisposedException (nameof (PendingRequestManager<,>));
		}

		readonly struct PendingRequestManagerCreate (
			PendingRequestManager<TKey, TValue>? pendingRequestManager, TKey key
		) : IDisposable {

			readonly PendingRequestManager<TKey, TValue>? PendingRequestManager = pendingRequestManager;
			readonly TKey Key = key;

			public void Dispose () {
				PendingRequestManager?.TrySetCanceled (Key);
			}

		}

		sealed class PendingRequest (
			TaskCompletionSource<TValue> taskCompletionSource,
			CancellationTokenSource cancellationTokenSource, CancellationTokenSource cancellationTokenSource1,
			CancellationToken cancellationToken
		) : IDisposable {

			public CancellationTokenRegistration? CancellationTokenRegistration { get; set; }

			readonly TaskCompletionSource<TValue> TaskCompletionSource = taskCompletionSource;
			readonly CancellationTokenSource CancellationTokenSource = cancellationTokenSource;
			readonly CancellationTokenSource CancellationTokenSource1 = cancellationTokenSource1;
			readonly CancellationToken CancellationToken = cancellationToken;
			int State;

			public void Dispose () {
				if (Interlocked.Exchange (ref State, 1) != 0) {
					return;
				}
				CancellationTokenRegistration?.Dispose ();
				CancellationTokenSource?.Dispose ();
				CancellationTokenSource1?.Dispose ();
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