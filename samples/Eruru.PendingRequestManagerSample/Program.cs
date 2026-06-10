using System.Text.Json.Nodes;
using System.Threading.Channels;
using Eruru.PendingRequestManager;

namespace Eruru.PendingRequestManagerSample;

sealed internal class Program {

	static async Task Main () {
		// 创建管理器
		// Create manager
		using var pendingRequestManager = new PendingRequestManager<int, string> () {
			// 设置 Task 的超时时间
			// Set the timeout duration for tasks
			Timeout = TimeSpan.FromSeconds (60)
		};
		var channel = Channel.CreateUnbounded<JsonObject> ();
		var id = 0;
		await Task.WhenAll (
			BeginRquestAsync (),
			BeginReceiveAsync ()
		).ConfigureAwait (false);
		Task BeginRquestAsync () {
			return Task.WhenAll (Enumerable.Range (0, 3).Select (async i => {
				var key = Interlocked.Increment (ref id);
				using var cancellationTokenSource = new CancellationTokenSource (TimeSpan.FromMilliseconds (2000));
				// 尝试创建 Task
				// Try to create a task
				using var _ = pendingRequestManager.Create (
					// 避免和正在等待中的 Key 重复
					// Avoid duplicate keys with pending requests
					key
					, out var task, state: key
					// 设置该 Task 的超时时间
					// Set a timeout for this task
					, cancellationToken: cancellationTokenSource.Token
				);
				if (task == null) {
					return;
				}
				Console.WriteLine ($"{DateTime.Now:O} Send request by {nameof (key)}: {key}");
				await channel.Writer.WriteAsync (new () {
					{ "id", key }
				}).ConfigureAwait (false);
				// 等待 Task 获取结果
				// Await the task result
				var result = await task.ConfigureAwait (false);
				Console.WriteLine ($"{DateTime.Now:O} Request {nameof (key)}: {task.AsyncState} received response {nameof (result)}: {result}");
			}));
		}
		async Task BeginReceiveAsync () {
			for (var i = 0; i < 3; i++) {
				var jsonObject = await channel.Reader.ReadAsync ().ConfigureAwait (false);
				await Task.Delay (500).ConfigureAwait (false);
				if (!jsonObject.TryGetPropertyValue ("id", out var id) || id == null) {
					continue;
				}
				// 为该 Key 对应的 Task 设置结果，如果未触发则由超时时间自动取消 Task 避免无限期等待
				// Set the result for the task associated with the key.
				// If no result is provided, the task will be automatically canceled by timeout to avoid waiting indefinitely.
				pendingRequestManager.TrySetResult (id.GetValue<int> (), $"{nameof (PendingRequestManager)} {id}");
			}
		}
	}

}