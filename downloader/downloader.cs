using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

public class RobustDownloader
{
    // === 配置参数 ===
    private const int MAX_RETRIES = 20;        // 重试次数
    private const int UI_UPDATE_RATE_MS = 500; // UI刷新频率
    private const int SPEED_WINDOW_SEC = 3;    // 速度平滑窗口(秒)
    private const int STALL_TIMEOUT_MINUTES = 3; // 僵死超时时间(分钟)

    // === 运行时参数 ===
    private static int _maxBufferCount; 
    private static int _blockSizeBytes;
    private static string _savePath = "";
    private static string _downloadingPath = "";
    private static string _configPath = "";
    private static long _totalFileSize = 0;
    private static string[] _originalArgs; // 保存原始参数用于重启

    // === 服务器时间 ===
    private static DateTime? _serverLastModifiedUtc = null;

    // === 状态控制 ===
    private static long _nextWriteOffset = 0; 
    private static readonly ConcurrentDictionary<long, byte[]> _buffer = new ConcurrentDictionary<long, byte[]>();
    
    // === 信号量 ===
    private static SemaphoreSlim _downloadSlots; 
    private static SemaphoreSlim _bufferSlots;   
    private static readonly object _configLock = new object();

    // === 统计与速度计算 ===
    private static long _totalBytesWritten = 0; // 磁盘落盘量
    private static long _totalNetworkBytes = 0; // 实时网络流量
    private static Stopwatch _globalStopwatch;
    
    // 速度计算滑动窗口
    private static readonly Queue<(double Time, long Bytes)> _speedSamples = new Queue<(double, long)>();

    // === 下载管理器 ===
    private static DownloadManager _downloadManager;

    public static async Task Main(string[] args)
    {
        _originalArgs = args; // 保存参数

        if (args.Length < 4)
        {
            PrintColor("Usage: downloader \"url\" \"save_path\" thread_count block_mb [--crc-only]", ConsoleColor.Yellow);
            return;
        }

        string url = args[0];
        _savePath = args[1];
        _downloadingPath = _savePath + ".downloading";
        _configPath = _savePath + ".cfg";
        int threadCount = int.Parse(args[2]);
        int blockSizeMb = int.Parse(args[3]);

        // 检查是否只执行 CRC64
        bool crcOnly = args.Any(a => a.Equals("--crc-only", StringComparison.OrdinalIgnoreCase));

        if (!crcOnly && File.Exists(_savePath))
        {
            PrintColor($"⚠️ Target file already exists, skipping download: {_savePath}", ConsoleColor.Yellow);
            return;
        }

        _blockSizeBytes = blockSizeMb * 1024 * 1024;
        _maxBufferCount = Math.Max(threadCount * 2, 32);

        _downloadSlots = new SemaphoreSlim(threadCount, threadCount);
        _bufferSlots = new SemaphoreSlim(_maxBufferCount, _maxBufferCount);

        var socketsHandler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = threadCount + 10 // 稍微增加一点冗余连接数，防止重启瞬间耗尽
        };
        var httpClient = new HttpClient(socketsHandler) { Timeout = TimeSpan.FromHours(24) };
        // 设置默认 User-Agent
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36");

        try
        {
            Console.Clear();
            PrintColor("=== Robust Downloader v4.5 (Fixed Logic) ===", ConsoleColor.Cyan);
            Console.WriteLine($"URL: {url}");
            Console.WriteLine($"Out: {_savePath}\n");

            // 1. 初始化 & 提取 CRC64
            bool supportsRange = await InitializeDownloadAsync(httpClient, url, crcOnly);

            // 如果只要 CRC64，则直接退出
            if (crcOnly)
            {
                PrintColor("\n✅ CRC64 extraction completed. Exiting as --crc-only is set.", ConsoleColor.Green);
                return;
            }

            if (!supportsRange)
            {
                PrintColor("⚠️  WARNING: Server does not support Range. Switching to single-threaded mode.", ConsoleColor.Red);
                await SingleThreadDownload(httpClient, url);
                return;
            }

            LoadResumeOffset();
            PrepareDiskSpace();

            // 移除这里原来的 GenerateChunks，改为在 Manager 内部动态生成，防止重启时逻辑不一致
            long remainingBytes = _totalFileSize - _nextWriteOffset;

            // 初始化网络计数
            _totalNetworkBytes = _nextWriteOffset;

            Console.WriteLine($"Total Size:      {FormatSize(_totalFileSize)}");
            Console.WriteLine($"Resuming From:   {FormatSize(_nextWriteOffset)} ({(_nextWriteOffset/(double)_totalFileSize):P1})");
            Console.WriteLine($"Remaining:       {FormatSize(remainingBytes)}");
            Console.WriteLine($"Threads:         {threadCount}");

            PrintColor("\n=== Starting Download ===", ConsoleColor.Green);
            _globalStopwatch = Stopwatch.StartNew();

            // 初始化下载管理器
            _downloadManager = new DownloadManager(url, threadCount);

            var writerTask = Task.Run(WriterLoop);

            // 启动 UI & 看门狗线程
            var uiTask = Task.Run(() => UILoop());

            // 启动下载管道（可软重启）
            await _downloadManager.StartAsync();

            await writerTask;

            // 正常结束
            if (_totalBytesWritten == _totalFileSize)
            {
                if (File.Exists(_configPath)) File.Delete(_configPath);

                if (File.Exists(_savePath)) File.Delete(_savePath);
                File.Move(_downloadingPath, _savePath);

                if (_serverLastModifiedUtc.HasValue)
                {
                    File.SetCreationTimeUtc(_savePath, _serverLastModifiedUtc.Value);
                    File.SetLastWriteTimeUtc(_savePath, _serverLastModifiedUtc.Value);
                }

                Console.WriteLine();
                PrintColor($"\n✅ Download Completed Successfully!", ConsoleColor.Green);
                PrintColor($"Avg Speed: {FormatSize((long)(_totalFileSize / _globalStopwatch.Elapsed.TotalSeconds))}/s", ConsoleColor.Gray);
                PrintColor($"Total Time: {_globalStopwatch.Elapsed:hh\\:mm\\:ss}", ConsoleColor.Gray);
            }
            else
            {
                PrintColor($"\n❌ Error: Size mismatch. Written: {_totalBytesWritten}, Expected: {_totalFileSize}", ConsoleColor.Red);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine();
            PrintColor($"\n❌ Fatal Error: {ex.Message}", ConsoleColor.Red);
        }
    }

    // ======================================
    // ===== 原有方法必须在 DownloadManager 外部 =====
    // ======================================

    private static async Task<bool> InitializeDownloadAsync(HttpClient client, string url, bool crcOnly = false)
    {
        Console.WriteLine("--- Connecting to server... ---");
        var request = new HttpRequestMessage(HttpMethod.Get, url);

        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        response.EnsureSuccessStatusCode();

        PrintColor("--- Server Headers ---", ConsoleColor.DarkGray);
        foreach (var header in response.Headers)
            Console.WriteLine($"{header.Key}: {string.Join(", ", header.Value)}");
        foreach (var header in response.Content.Headers)
            Console.WriteLine($"{header.Key}: {string.Join(", ", header.Value)}");
        Console.WriteLine("----------------------");

        // === 提取 LastModified ===
        if (response.Content.Headers.LastModified.HasValue)
            _serverLastModifiedUtc = response.Content.Headers.LastModified.Value.UtcDateTime;

        // === 提取 CRC64 ===
        if (response.Headers.TryGetValues("x-cos-hash-crc64ecma", out var crcValues))
        {
            string crcValue = crcValues.FirstOrDefault();
            if (!string.IsNullOrEmpty(crcValue))
            {
                string crcFileName = _savePath + ".crc64";
                string fileName = Path.GetFileName(_savePath);
                string content = $"{fileName}===={crcValue}";
                if (!File.Exists(crcFileName) || File.ReadAllText(crcFileName) != content)
                {
                    await File.WriteAllTextAsync(crcFileName, content);
                    PrintColor($"[CRC64] Value extracted and saved to: {Path.GetFileName(crcFileName)}", ConsoleColor.Cyan);
                }
            }
        }
        Console.WriteLine();

        if (response.Content.Headers.ContentLength.HasValue)
            _totalFileSize = response.Content.Headers.ContentLength.Value;

        if (crcOnly) return false;

        // === 探测是否支持 Range 请求 ===
        bool supportsRange = false;
        try
        {
            var rangeRequest = new HttpRequestMessage(HttpMethod.Get, url);
            rangeRequest.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(0, 0);
            using var rangeResponse = await client.SendAsync(rangeRequest, HttpCompletionOption.ResponseHeadersRead);

            if (rangeResponse.StatusCode == System.Net.HttpStatusCode.PartialContent)
                supportsRange = true;
        }
        catch { supportsRange = false; }

        return supportsRange;
    }

    // 修改：增加了 CancellationToken 参数
    private static async Task DownloadChunkWithRetry(HttpClient client, string url, Chunk chunk, CancellationToken token)
    {
        int retry = 0;
        while (retry < MAX_RETRIES)
        {
            // 如果已经被取消，直接抛出，不要再尝试重连
            token.ThrowIfCancellationRequested();

            try
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(chunk.Start, chunk.End);

                // 重点：将 Token 传递给 SendAsync，这样软重启时能立即断开连接
                using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
                if (response.StatusCode != HttpStatusCode.PartialContent) 
                    throw new Exception($"Invalid Status Code: {response.StatusCode}");

                using var stream = await response.Content.ReadAsStreamAsync(token);
                
                long expectedSize = chunk.End - chunk.Start + 1;
                byte[] data = new byte[expectedSize];
                
                int totalRead = 0;
                while (totalRead < expectedSize)
                {
                    // 重点：将 Token 传递给 ReadAsync
                    int read = await stream.ReadAsync(data, totalRead, (int)(expectedSize - totalRead), token);
                    if (read == 0) break;
                    totalRead += read;
                    Interlocked.Add(ref _totalNetworkBytes, read);
                }

                if (totalRead != expectedSize) throw new IOException("Stream ended early");

                if (!_buffer.TryAdd(chunk.Start, data)) { }
                return; // 成功下载并加入 Buffer
            }
            catch (OperationCanceledException)
            {
                throw; // 向上抛出取消异常
            }
            catch (Exception)
            {
                retry++;
                if (retry >= MAX_RETRIES) throw; 
                try { await Task.Delay(Math.Min(5000, 1000 * retry), token); } catch { }
            }
        }
    }

    private static void WriterLoop()
    {
        // 保持原来的逻辑：顺序写入，这对机械硬盘至关重要
        using var fs = new FileStream(_downloadingPath, FileMode.Open, FileAccess.Write, FileShare.Read);
        fs.Seek(_nextWriteOffset, SeekOrigin.Begin);

        long unflushedBytes = 0; 
        const long FLUSH_THRESHOLD = 32 * 1024 * 1024; 

        while (_totalBytesWritten < _totalFileSize)
        {
            if (_buffer.TryGetValue(_nextWriteOffset, out byte[] data))
            {
                fs.Write(data, 0, data.Length);
                _nextWriteOffset += data.Length;
                _totalBytesWritten += data.Length;
                unflushedBytes += data.Length;

                // 移除已写入的块
                _buffer.TryRemove(_nextWriteOffset - data.Length, out _);
                
                // 重点：Writer 负责释放“成功消费”的 Buffer 配额
                _bufferSlots.Release();

                if (unflushedBytes >= FLUSH_THRESHOLD || _totalBytesWritten == _totalFileSize)
                {
                    fs.Flush(true);
                    unflushedBytes = 0;
                    UpdateConfigFile(_nextWriteOffset);
                }
            }
            else
            {
                Thread.Sleep(20); // 机械硬盘不建议轮询太快
            }
        }
        fs.Flush(true);
        UpdateConfigFile(_totalFileSize);
    }

    private static async Task UILoop()
    {
        long lastNetworkBytes = 0;
        DateTime lastActivityTime = DateTime.Now;

        while (_totalBytesWritten < _totalFileSize)
        {
            long currentBytes = Interlocked.Read(ref _totalNetworkBytes);
            
            if (currentBytes > lastNetworkBytes)
            {
                lastNetworkBytes = currentBytes;
                lastActivityTime = DateTime.Now;
            }
            else
            {
                var stalledDuration = DateTime.Now - lastActivityTime;
                if (stalledDuration.TotalMinutes >= STALL_TIMEOUT_MINUTES)
                {
                    Console.WriteLine();
                    PrintColor($"\n⚠️  STALL DETECTED! Download speed has been 0 for {STALL_TIMEOUT_MINUTES} minutes.", ConsoleColor.Red);
                    PrintColor("🔄 Restarting downloader automatically...", ConsoleColor.Yellow);
                    
                    // 触发软重启
                    _downloadManager.SoftRestart();
                    
                    // 稍微重置一下时间，避免连续触发
                    lastActivityTime = DateTime.Now; 
                }
            }

            UpdateUI(currentBytes, lastActivityTime);
            await Task.Delay(UI_UPDATE_RATE_MS);
        }
        UpdateUI(Interlocked.Read(ref _totalNetworkBytes), DateTime.Now); 
    }

    private static async Task SingleThreadDownload(HttpClient client, string url)
    {
        // 单线程模式逻辑保持不变
        using var response = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
        using var stream = await response.Content.ReadAsStreamAsync();
        using var fs = new FileStream(_downloadingPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        
        var buffer = new byte[81920];
        int read;
        long total = 0;
        var sw = Stopwatch.StartNew();
        long lastTime = 0;
        long lastBytes = 0;
        _totalFileSize = response.Content.Headers.ContentLength ?? 0;

        while ((read = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            await fs.WriteAsync(buffer, 0, read);
            total += read;
            if (sw.ElapsedMilliseconds - lastTime > 1000)
            {
                double speed = (total - lastBytes) / ((sw.ElapsedMilliseconds - lastTime) / 1000.0);
                Console.Write($"\rDownloading: {FormatSize(total)} / {FormatSize(_totalFileSize)} | Speed: {FormatSize((long)speed)}/s   ");
                lastTime = sw.ElapsedMilliseconds;
                lastBytes = total;
            }
        }
        Console.WriteLine("\nDone.");
    }

    private static void LoadResumeOffset()
    {
        if (File.Exists(_configPath) && File.Exists(_downloadingPath))
        {
            try
            {
                var lines = File.ReadAllLines(_configPath);
                if (lines.Length > 0 && long.TryParse(lines[0], out long savedOffset))
                {
                    if (savedOffset <= _totalFileSize && new FileInfo(_downloadingPath).Length >= savedOffset)
                    {
                        _nextWriteOffset = savedOffset;
                        _totalBytesWritten = savedOffset;
                        return;
                    }
                }
            }
            catch { }
        }
    }

    private static void PrepareDiskSpace()
    {
        if (!File.Exists(_downloadingPath) || new FileInfo(_downloadingPath).Length != _totalFileSize)
        {
            Console.WriteLine("Allocating disk space...");
            using (var fs = new FileStream(_downloadingPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                fs.SetLength(_totalFileSize);
        }
    }

    private static void UpdateConfigFile(long offset)
    {
        lock (_configLock) File.WriteAllText(_configPath, offset.ToString());
    }

    // 辅助方法：生成需要下载的块（在 Manager 内部调用）
    private static ConcurrentQueue<Chunk> GenerateChunksQueue(long startOffset)
    {
        var queue = new ConcurrentQueue<Chunk>();
        long current = startOffset;
        
        // 我们需要跳过已经在内存 Buffer 中但还没写入磁盘的块
        // 防止重复下载导致浪费带宽
        var existingKeys = new HashSet<long>(_buffer.Keys);

        while (current < _totalFileSize)
        {
            if (!existingKeys.Contains(current))
            {
                long end = Math.Min(current + _blockSizeBytes - 1, _totalFileSize - 1);
                queue.Enqueue(new Chunk { Start = current, End = end });
            }
            current += _blockSizeBytes;
        }
        return queue;
    }

    private static void UpdateUI(long currentNetworkBytes, DateTime lastActivityTime)
    {
        double nowSeconds = _globalStopwatch.Elapsed.TotalSeconds;

        lock (_speedSamples)
        {
            _speedSamples.Enqueue((nowSeconds, currentNetworkBytes));
            while (_speedSamples.Count > 0 && nowSeconds - _speedSamples.Peek().Time > SPEED_WINDOW_SEC)
                _speedSamples.Dequeue();
        }

        double speed = 0;
        lock (_speedSamples)
        {
            if (_speedSamples.Count >= 2)
            {
                var first = _speedSamples.Peek();
                var last = _speedSamples.Last(); 
                if (last.Time - first.Time > 0.1)
                    speed = (last.Bytes - first.Bytes) / (last.Time - first.Time);
            }
        }

        long remainingBytes = _totalFileSize - _totalBytesWritten; 
        double progressPct = (double)_totalBytesWritten / _totalFileSize;

        TimeSpan eta = TimeSpan.Zero;
        if (speed > 0) try { eta = TimeSpan.FromSeconds(remainingBytes / speed); } catch { }

        int barWidth = 25;
        int filled = (int)(progressPct * barWidth);
        string bar = "[" + new string('=', filled) + ">" + new string(' ', Math.Max(0, barWidth - filled - 1)) + "]";
        if (filled >= barWidth) bar = "[" + new string('=', barWidth) + "]";

        string speedStr = $"{FormatSize((long)speed)}/s".PadRight(10);
        
        if ((DateTime.Now - lastActivityTime).TotalSeconds > 10)
            speedStr = "STALLED!".PadRight(10);

        Console.Write($"\r{bar} {progressPct:P1} | {FormatSize(_totalBytesWritten)}/{FormatSize(_totalFileSize)} | {speedStr} | ETA: {eta:hh\\:mm\\:ss}   ");
    }

    private static string FormatSize(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1) { order++; len /= 1024; }
        return $"{len:0.00} {sizes[order]}";
    }

    private static void PrintColor(string msg, ConsoleColor color)
    {
        var prev = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.WriteLine(msg);
        Console.ForegroundColor = prev;
    }

    private struct Chunk { public long Start; public long End; }

    // ===== DownloadManager 负责软重启 =====
    private class DownloadManager
    {
        private string _url;
        private int _threadCount;
        private CancellationTokenSource _cts;
        private HttpClient _client;
        private volatile bool _isRestarting = false; // 标记是否正在重启中

        public DownloadManager(string url, int threadCount)
        {
            _url = url;
            _threadCount = threadCount;
            _cts = new CancellationTokenSource();
            _client = CreateHttpClient();
        }

        private HttpClient CreateHttpClient()
        {
            var handler = new SocketsHttpHandler
            {
                PooledConnectionLifetime = TimeSpan.FromMinutes(2),
                MaxConnectionsPerServer = _threadCount + 10 // 稍微给多一点，防止重启时连接池溢出
            };
            return new HttpClient(handler) { Timeout = TimeSpan.FromHours(24) };
        }

        public async Task StartAsync()
        {
            // 只要磁盘没写完，就一直循环（大循环负责处理重启）
            while (_totalBytesWritten < _totalFileSize)
            {
                _isRestarting = false;
                
                // 1. 根据当前的写入进度重新生成队列
                var chunksQueue = GenerateChunksQueue(_nextWriteOffset);

                // 如果队列为空，说明剩余的块都已经下载在 buffer 里了，只是还没写入硬盘。
                // 此时不需要启动下载任务，只需要等待 WriterLoop 工作。
                if (chunksQueue.IsEmpty)
                {
                    await Task.Delay(500); // 挂起 500ms，把 CPU 让给 WriterLoop
                    continue; // 跳过本次循环，重新检查 _totalBytesWritten
                }

                var activeTasks = new List<Task>();

                PrintColor($"\n[DownloadManager] Starting loop. Chunks remaining: {chunksQueue.Count}", ConsoleColor.DarkGray);

                try
                {
                    while (!chunksQueue.IsEmpty && !_isRestarting)
                    {
                        // 2. 严格的信号量管理逻辑，防止死锁
                        bool acquiredBuffer = false;
                        bool acquiredThread = false;

                        try
                        {
                            await _bufferSlots.WaitAsync(_cts.Token);
                            acquiredBuffer = true;

                            await _downloadSlots.WaitAsync(_cts.Token);
                            acquiredThread = true;

                            if (chunksQueue.TryDequeue(out Chunk chunk))
                            {
                                activeTasks.Add(Task.Run(async () =>
                                {
                                    bool success = false;
                                    try
                                    {
                                        await DownloadChunkWithRetry(_client, _url, chunk, _cts.Token);
                                        success = true;
                                    }
                                    catch (OperationCanceledException) { }
                                    catch (Exception) { }
                                    finally
                                    {
                                        _downloadSlots.Release();
                                        // 如果下载失败/取消，数据没进 buffer，必须在此释放 buffer 配额
                                        if (!success) _bufferSlots.Release();
                                    }
                                }, _cts.Token));
                            }
                            else
                            {
                                _downloadSlots.Release();
                                _bufferSlots.Release();
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            if (acquiredThread) _downloadSlots.Release();
                            if (acquiredBuffer) _bufferSlots.Release();
                            throw;
                        }

                        activeTasks.RemoveAll(t => t.IsCompleted);
                    }

                    await Task.WhenAll(activeTasks);
                }
                catch (OperationCanceledException)
                {
                    PrintColor("\n🔄 DownloadManager is resetting connection pool...", ConsoleColor.Yellow);
                    try { await Task.WhenAll(activeTasks); } catch { }
                    _cts.Dispose();
                    _cts = new CancellationTokenSource();
                    _client.Dispose();
                    _client = CreateHttpClient();
                    PrintColor("✅ Reset complete. Resuming download...", ConsoleColor.Yellow);
                }
            }
        }

        public void SoftRestart()
        {
            if (!_isRestarting)
            {
                _isRestarting = true;
                _cts.Cancel(); // 这会触发 StartAsync 内部的 catch (OperationCanceledException)
            }
        }
    }
}