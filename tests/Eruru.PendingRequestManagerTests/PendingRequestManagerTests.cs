using System.Collections.Concurrent;
using Eruru.PendingRequestManager;

namespace Eruru.PendingRequestManagerTests {

#pragma warning disable CA1724 // 类型名与命名空间名称整体或部分冲突
	public class PendingRequestManagerTests (ITestOutputHelper testOutputHelper) {
#pragma warning restore CA1724 // 类型名与命名空间名称整体或部分冲突

		readonly ITestOutputHelper TestOutputHelper = testOutputHelper;

		[Fact]
		public void TryCreateDuplicateKey () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			var key = int.MaxValue;
			Assert.True (pendingRequestManager.TryCreate (key, out var task));
			Assert.False (pendingRequestManager.TryCreate (key, out _));
		}

		[Fact]
		public async Task TrySetResult () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			var key = int.MaxValue;
			Assert.True (pendingRequestManager.TryCreate (key, out var task) && task != null);
			Assert.True (pendingRequestManager.TrySetResult (key, nameof (PendingRequestManager)));
			Assert.Equal (nameof (PendingRequestManager), await task.ConfigureAwait (true));
		}

		[Fact]
		public Task TrySetException () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			var key = int.MaxValue;
			Assert.True (pendingRequestManager.TryCreate (key, out var task) && task != null);
			Assert.True (pendingRequestManager.TrySetException (key, new HttpRequestException ()));
			return Assert.ThrowsAsync<HttpRequestException> (() => task);
		}

		[Fact]
		public Task TrySetCanceled () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			var key = int.MaxValue;
			Assert.True (pendingRequestManager.TryCreate (key, out var task) && task != null);
			Assert.True (pendingRequestManager.TrySetCanceled (key));
			return Assert.ThrowsAsync<TaskCanceledException> (() => task);
		}

		[Fact]
		public void TrySetResultWithoutTryCreate () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			var key = int.MaxValue;
			Assert.False (pendingRequestManager.TrySetResult (key, nameof (PendingRequestManager)));
		}

		[Fact]
		public void TrySetExceptionWithoutTryCreate () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			var key = int.MaxValue;
			Assert.False (pendingRequestManager.TrySetException (key, new HttpRequestException ()));
		}

		[Fact]
		public void TrySetCanceledWithoutTryCreate () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			var key = int.MaxValue;
			Assert.False (pendingRequestManager.TrySetCanceled (key));
		}

		[Fact]
		public async Task WaitingTimeoutAfterTryCreate () {
			using var pendingRequestManager = new PendingRequestManager<int, string> () { Timeout = TimeSpan.FromMilliseconds (0) };
			var key = int.MaxValue;
			Assert.True (pendingRequestManager.TryCreate (key, out var task) && task != null);
			await Assert.ThrowsAsync<TaskCanceledException> (() => task);
		}

		[Fact]
		public async Task WaitingCustomTimeoutAfterTryCreate () {
			using var pendingRequestManager = new PendingRequestManager<int, string> ();
			using var cancellationTokenSource = new CancellationTokenSource (TimeSpan.FromMilliseconds (100));
			var key = int.MaxValue;
			Assert.True (pendingRequestManager.TryCreate (
				key, out var task, cancellationToken: cancellationTokenSource.Token
			) && task != null);
			await Assert.ThrowsAsync<TaskCanceledException> (() => task);
		}

		[Fact]
		public async Task DisposeAfterTryCreate () {
			using var pendingRequestManager = new PendingRequestManager<int, string> () { Timeout = TimeSpan.FromMilliseconds (100) };
			var key = int.MaxValue;
			Assert.True (pendingRequestManager.TryCreate (key, out var task) && task != null);
			pendingRequestManager.Dispose ();
			await Assert.ThrowsAsync<ObjectDisposedException> (() => task);
		}

		[Fact]
		public async Task ThreadSafe () {
			using var cancellationTokenSource = new CancellationTokenSource (TimeSpan.FromMilliseconds (1000));
			PendingRequestManager<int, string>? pendingRequestManager = null;
			PendingRequestManager<int, string>? newPendingRequestManager = null;
			ConcurrentQueue<PendingRequestManager<int, string>> pendingRequestManagers = [];
			var useCounter = 0;
			var disposeCounter = 0;
			await Task.WhenAll (
				Task.Run (async () => {
					while (!cancellationTokenSource.Token.IsCancellationRequested) {
						var oldPendingRequestManager = Interlocked.CompareExchange (ref pendingRequestManager, null, null);
						if (oldPendingRequestManager == null) {
							continue;
						}
						var key = Interlocked.Increment (ref useCounter);
						try {
							if (!oldPendingRequestManager.TryCreate (key, out var task) || task == null) {
								continue;
							}
							oldPendingRequestManager.TrySetResult (key, nameof (PendingRequestManager));
							Assert.Equal (nameof (PendingRequestManager), await task.ConfigureAwait (false));
						} catch (ObjectDisposedException) {

						} catch (OperationCanceledException) {

						}
					}
				}, TestContext.Current.CancellationToken)
				, Task.Run (async () => {
					while (!cancellationTokenSource.Token.IsCancellationRequested) {
						newPendingRequestManager = new PendingRequestManager<int, string> ();
						var oldPendingRequestManager = Interlocked.Exchange (ref pendingRequestManager, newPendingRequestManager);
						oldPendingRequestManager?.Dispose ();
						Interlocked.Increment (ref disposeCounter);
						pendingRequestManagers.Enqueue (newPendingRequestManager);
						await Task.Delay (TimeSpan.FromMilliseconds (1)).ConfigureAwait (false);
					}
				}, TestContext.Current.CancellationToken)
			);
			newPendingRequestManager?.Dispose ();
			TestOutputHelper.WriteLine ($"{nameof (useCounter)}: {useCounter:#,0.##} {nameof (disposeCounter)}: {disposeCounter:#,0.##}");
			foreach (var item in pendingRequestManagers) {
				Assert.Equal (0, item.Count);
			}
		}

	}

}