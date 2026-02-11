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
    // ==========================================
    // 配置参数 (Configuration)
    // ==========================================
    private const int MAX_RETRIES = 50;             // 大幅增加重试次数，因为现在有了切片模式，重试不再可怕
    private const int UI_UPDATE_RATE_MS = 500;      // UI 刷新频率 (毫秒)
    private const int SPEED_WINDOW_SEC = 3;         // 速度计算的滑动窗口 (秒)
    private const int STALL_TIMEOUT_MINUTES = 1;    // 判定下载僵死的超时时间 (分钟)
    private const int READ_TIMEOUT_SECONDS = 30;    // 读取流超时时间 (防止僵尸连接)

    // ==========================================
    // 运行时状态 (Runtime State)
    // ==========================================
    private static int _maxBufferCount;
    private static int _blockSizeBytes;
    private static string _savePath = "";
    private static string _downloadingPath = "";
    private static string _configPath = "";
    private static long _totalFileSize = 0;
    private static string[] _originalArgs;
    
    // 用于 UI 显示当前的诊断状态
    private static string _diagStatus = "Initializing...";
    
    // Basic Auth header
    private static string _basicAuthHeader = null;

    // 服务器元数据
    private static DateTime? _serverLastModifiedUtc = null;

    // 偏移量控制
    private static long _nextWriteOffset = 0;
    private static readonly ConcurrentDictionary<long, byte[]> _buffer = new ConcurrentDictionary<long, byte[]>();

    // 并发控制 (信号量)
    private static SemaphoreSlim _downloadSlots;
    private static SemaphoreSlim _bufferSlots;
    private static readonly object _configLock = new object();

    // 统计数据
    private static long _totalBytesWritten = 0;
    private static long _totalNetworkBytes = 0;
    private static Stopwatch _globalStopwatch;
    private static readonly Queue<(double Time, long Bytes)> _speedSamples = new Queue<(double, long)>();

    // 下载管理器
    private static DownloadManager _downloadManager;

    public static async Task Main(string[] args)
    {
        _originalArgs = args;

        if (args.Length < 4)
        {
            LogToConsole("Usage: downloader \"url\" \"save_path\" thread_count block_mb [--crc-only]", ConsoleColor.Yellow);
            return;
        }

        string rawUrl = args[0];
        var uri = new Uri(rawUrl);

        string url = uri.GetLeftPart(UriPartial.Path);

        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var encoded = Convert.ToBase64String(
                System.Text.Encoding.UTF8.GetBytes(uri.UserInfo));
            _basicAuthHeader = $"Basic {encoded}";
        }
        _savePath = args[1];
        _downloadingPath = _savePath + ".downloading";
        _configPath = _savePath + ".cfg";
        int threadCount = int.Parse(args[2]);
        double blockSizeMb = double.Parse(args[3]);

        bool crcOnly = args.Any(a => a.Equals("--crc-only", StringComparison.OrdinalIgnoreCase));

        if (!crcOnly && File.Exists(_savePath))
        {
            LogToConsole($"Target file already exists, skipping download: {_savePath}", ConsoleColor.Yellow);
            return;
        }

        _blockSizeBytes = (int)(blockSizeMb * 1024 * 1024);
        _maxBufferCount = Math.Max(threadCount * 2, 32);

        _downloadSlots = new SemaphoreSlim(threadCount, threadCount);
        _bufferSlots = new SemaphoreSlim(_maxBufferCount, _maxBufferCount);

        // 配置连接池，适当轮换端口
        var socketsHandler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = threadCount + 20
        };
        var httpClient = new HttpClient(socketsHandler) { Timeout = TimeSpan.FromHours(24) };
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36");
        
        if (_basicAuthHeader != null)
        {
            httpClient.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Basic",
                    _basicAuthHeader.Substring(6));
        }

        try
        {
            // 只有在未重定向输出(有真实控制台窗口)时才清屏
            if (!Console.IsOutputRedirected)
            {
                try { Console.Clear(); } catch { }
            }
            
            LogToConsole("=== Robust Downloader v4.8 (Surgical Mode) ===", ConsoleColor.Cyan);
            Console.WriteLine($"URL: {url}");
            Console.WriteLine($"Out: {_savePath}\n");

            // 初始化连接并获取元数据
            bool supportsRange = await InitializeDownloadAsync(httpClient, url, crcOnly);

            if (crcOnly)
            {
                LogToConsole("\nCRC64 extraction completed. Exiting.", ConsoleColor.Green);
                return;
            }

            if (!supportsRange)
            {
                LogToConsole("WARNING: Server does not support Range. Switching to single-threaded mode.", ConsoleColor.Red);
                return;
            }

            LoadResumeOffset();
            PrepareDiskSpace();

            // 记录本次会话开始时的偏移量，用于准确计算平均速度
            long sessionStartOffset = _nextWriteOffset;
            long remainingBytes = _totalFileSize - _nextWriteOffset;
            _totalNetworkBytes = _nextWriteOffset;

            Console.WriteLine($"Total Size:      {FormatSize(_totalFileSize)}");
            Console.WriteLine($"Resuming From:   {FormatSize(_nextWriteOffset)} ({(_nextWriteOffset / (double)_totalFileSize):P1})");
            Console.WriteLine($"Remaining:       {FormatSize(remainingBytes)}");
            Console.WriteLine($"Threads:         {threadCount}");
            Console.WriteLine($"Mode:            Adaptive Fragmentation Enabled");

            LogToConsole("\n=== Starting Download ===", ConsoleColor.Green);
            _globalStopwatch = Stopwatch.StartNew();

            _downloadManager = new DownloadManager(url, threadCount);

            // 启动后台任务
            var writerTask = Task.Run(WriterLoop);
            var uiTask = Task.Run(() => UILoop());

            // 启动主下载循环
            await _downloadManager.StartAsync();

            await writerTask;

            // 完成处理
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

                // 计算平均速度
                long bytesDownloadedThisSession = _totalBytesWritten - sessionStartOffset;
                double elapsedSeconds = _globalStopwatch.Elapsed.TotalSeconds;
                long avgSpeed = elapsedSeconds > 0 ? (long)(bytesDownloadedThisSession / elapsedSeconds) : 0;

                Console.WriteLine();
                LogToConsole($"\nDownload Completed Successfully!", ConsoleColor.Green);
                LogToConsole($"Avg Speed: {FormatSize(avgSpeed)}/s", ConsoleColor.Gray);
                LogToConsole($"Total Time: {_globalStopwatch.Elapsed:hh\\:mm\\:ss}", ConsoleColor.Gray);
            }
            else
            {
                LogToConsole($"\nError: Size mismatch. Written: {_totalBytesWritten}, Expected: {_totalFileSize}", ConsoleColor.Red);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine();
            LogToConsole($"\nFatal Error: {ex.Message}", ConsoleColor.Red);
        }
    }

    // ======================================
    // 辅助方法 (Helper Methods)
    // ======================================

    private static async Task<bool> InitializeDownloadAsync(HttpClient client, string url, bool crcOnly = false)
    {
        Console.WriteLine("--- Connecting to server... ---");
        var request = new HttpRequestMessage(HttpMethod.Get, url);

        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        response.EnsureSuccessStatusCode();

        LogToConsole("--- Server Headers ---", ConsoleColor.DarkGray);
        foreach (var header in response.Headers)
            Console.WriteLine($"{header.Key}: {string.Join(", ", header.Value)}");
        foreach (var header in response.Content.Headers)
            Console.WriteLine($"{header.Key}: {string.Join(", ", header.Value)}");
        Console.WriteLine("----------------------");

        if (response.Content.Headers.LastModified.HasValue)
            _serverLastModifiedUtc = response.Content.Headers.LastModified.Value.UtcDateTime;

        // 尝试提取 CRC64
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
                    LogToConsole($"[CRC64] Saved to: {Path.GetFileName(crcFileName)}", ConsoleColor.Cyan);
                }
            }
        }
        Console.WriteLine();

        if (response.Content.Headers.ContentLength.HasValue)
            _totalFileSize = response.Content.Headers.ContentLength.Value;

        if (crcOnly) return false;

        // 检查 Range 支持
        bool supportsRange = false;
        try
        {
            var rangeRequest = new HttpRequestMessage(HttpMethod.Get, url);
            rangeRequest.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(0, 0);
            using var rangeResponse = await client.SendAsync(rangeRequest, HttpCompletionOption.ResponseHeadersRead);

            if (rangeResponse.StatusCode == HttpStatusCode.PartialContent)
                supportsRange = true;
        }
        catch { supportsRange = false; }

        return supportsRange;
    }

    /// <summary>
    /// 下载指定分块，包含重试逻辑和连接超时处理
    /// </summary>
    private static async Task DownloadChunkWithRetry(HttpClient client, string url, Chunk chunk, CancellationToken token)
    {
        int retry = 0;
        
        // 预分配整个块的内存
        long totalSize = chunk.End - chunk.Start + 1;
        byte[] data = new byte[totalSize];
        int bytesReceivedTotal = 0; // 当前已经下载到 data 里的字节数

        while (retry < MAX_RETRIES && bytesReceivedTotal < totalSize)
        {
            token.ThrowIfCancellationRequested();
            try
            {
                // 计算本次请求的范围
                long requestStart = chunk.Start + bytesReceivedTotal;
                long requestEnd = chunk.End;
                
                // 动态切片
                long remaining = requestEnd - requestStart + 1;
                long currentRequestLimit = remaining;

                if (retry > 2) currentRequestLimit = 1 * 1024 * 1024; // >2次重试：每次只下 1MB
                if (retry > 5) currentRequestLimit = 64 * 1024;  // >5次重试：每次只下 64KB
                if (retry > 8) currentRequestLimit = 32 * 1024;       // >8次重试：每次只下 32KB

                if (currentRequestLimit < remaining)
                {
                    requestEnd = requestStart + currentRequestLimit - 1;
                    // LogToConsole 可能会刷屏，这里不打印，但逻辑是在悄悄执行“手术”
                }
                // ===========================================

                var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(requestStart, requestEnd);

                // 连接建立超时设置
                using var ctsSend = CancellationTokenSource.CreateLinkedTokenSource(token);
                ctsSend.CancelAfter(TimeSpan.FromSeconds(20));

                using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ctsSend.Token);
                
                if (!response.IsSuccessStatusCode)
                {
                    if ((int)response.StatusCode == 429 || (int)response.StatusCode == 503)
                    {
                        retry++;
                        await Task.Delay(2000 * retry, token); // 指数退避
                        continue;
                    }
                    response.EnsureSuccessStatusCode();
                }

                using var stream = await response.Content.ReadAsStreamAsync(token);
                
                long expectedForThisRequest = requestEnd - requestStart + 1;
                int bytesReadForThisRequest = 0;

                while (bytesReadForThisRequest < expectedForThisRequest)
                {
                    // 为每次 ReadAsync 设置独立的超时监控，防止僵尸连接
                    using var ctsRead = CancellationTokenSource.CreateLinkedTokenSource(token);
                    ctsRead.CancelAfter(TimeSpan.FromSeconds(READ_TIMEOUT_SECONDS));

                    try 
                    {
                        // 直接写入 data 数组的正确偏移位置
                        int read = await stream.ReadAsync(data, bytesReceivedTotal, (int)(expectedForThisRequest - bytesReadForThisRequest), ctsRead.Token);
                        
                        if (read == 0) break; // 连接意外断开

                        bytesReceivedTotal += read;     // 总进度推进
                        bytesReadForThisRequest += read; // 本次请求进度推进
                        Interlocked.Add(ref _totalNetworkBytes, read);
                        
                        // if (retry > 0) retry = Math.Max(1, retry - 1); 
                    }
                    catch (OperationCanceledException)
                    {
                        if (token.IsCancellationRequested) throw;
                        throw new IOException($"Read timeout after {READ_TIMEOUT_SECONDS}s");
                    }
                }
                
                // 如果本次请求完美结束，循环会继续，处理剩下的部分（如果有）
                // 此时 bytesReceivedTotal 增加了，下一次循环的 requestStart 会自动后移
            }
            catch (Exception)
            {
                retry++;
                if (retry >= MAX_RETRIES) throw; // 真的没救了
                
                // 简单的退避等待
                try { await Task.Delay(500 + (retry * 200), token); } catch { }
            }
        }

        // 最终检查：是否填满了整个块
        if (bytesReceivedTotal != totalSize) 
            throw new IOException($"Chunk failed after {MAX_RETRIES} retries. Got {bytesReceivedTotal}/{totalSize}");

        // 成功！加入缓冲区
        if (!_buffer.TryAdd(chunk.Start, data)) { }
    }

    /// <summary>
    /// 顺序写入循环（针对机械硬盘优化）
    /// </summary>
    private static void WriterLoop()
    {
        using var fs = new FileStream(_downloadingPath, FileMode.Open, FileAccess.Write, FileShare.Read);
        fs.Seek(_nextWriteOffset, SeekOrigin.Begin);

        long unflushedBytes = 0; 
        const long FLUSH_THRESHOLD = 32 * 1024 * 1024; // 32MB 缓冲

        while (_totalBytesWritten < _totalFileSize)
        {
            if (_buffer.TryGetValue(_nextWriteOffset, out byte[] data))
            {
                fs.Write(data, 0, data.Length);
                _nextWriteOffset += data.Length;
                _totalBytesWritten += data.Length;
                unflushedBytes += data.Length;

                _buffer.TryRemove(_nextWriteOffset - data.Length, out _);
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
                // 等待数据到达
                Thread.Sleep(20);
            }
        }
        fs.Flush(true);
        UpdateConfigFile(_totalFileSize);
    }

    /// <summary>
    /// UI 刷新与卡顿检测线程
    /// </summary>
    private static async Task UILoop()
    {
        long lastNetworkBytes = 0;
        DateTime lastActivityTime = DateTime.Now;

        while (_totalBytesWritten < _totalFileSize)
        {
            long currentBytes = Interlocked.Read(ref _totalNetworkBytes);
            
            // --- 实时诊断逻辑 ---
            
            bool isBufferFull = _bufferSlots.CurrentCount == 0;
            bool areThreadsBusy = _downloadSlots.CurrentCount == 0;
            bool hasWriterBlock = _buffer.ContainsKey(_nextWriteOffset);
            
            if (currentBytes > lastNetworkBytes)
            {
                _diagStatus = "Running";
            }
            else
            {
                // 判定卡顿原因
                if (hasWriterBlock)
                {
                    _diagStatus = "Disk I/O Bottleneck";
                }
                else if (isBufferFull)
                {
                    _diagStatus = "DEADLOCK: Buffer Full & Missing Next Block";
                }
                else if (areThreadsBusy)
                {
                    _diagStatus = "Network Hang / Fragmenting";
                }
                else
                {
                    _diagStatus = "Idle / Queue Empty";
                }
            }

            // --- 卡顿处理 ---

            if (currentBytes > lastNetworkBytes)
            {
                lastNetworkBytes = currentBytes;
                lastActivityTime = DateTime.Now;
            }
            else
            {
                var stalledDuration = DateTime.Now - lastActivityTime;
                
                // 如果卡顿超过阈值（15秒检查一次）
                if (stalledDuration.TotalSeconds > 15)
                {
                    // 死锁解决：外科手术式移除
                    if (_diagStatus.Contains("DEADLOCK"))
                    {
                        ResolveDeadlock();
                        lastActivityTime = DateTime.Now; 
                    }
                    // 普通卡顿：软重启
    /// 通过移除内存中此时不需要的最远块来解除死锁
    /// 生成下载队列，严格限制窗口大小在 Buffer 容量内，防止填充无用的远端块
                    else if (stalledDuration.TotalMinutes >= STALL_TIMEOUT_MINUTES)
                    {
                        LogToConsole($"STALL DETECTED! Duration: {stalledDuration.TotalMinutes:F1} min. Reason: {_diagStatus}", ConsoleColor.Red);
                        LogToConsole("Initiating soft restart...", ConsoleColor.Yellow);
                        
                        _downloadManager.SoftRestart();
                        lastActivityTime = DateTime.Now; 
                    }
                }
            }

            UpdateUI(currentBytes, lastActivityTime);
            await Task.Delay(UI_UPDATE_RATE_MS);
        }
        UpdateUI(Interlocked.Read(ref _totalNetworkBytes), DateTime.Now); 
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

    /// <summary>
    /// 通过移除内存中此时不需要的最远块来解除死锁
    /// 生成下载队列，严格限制窗口大小在 Buffer 容量内，防止填充无用的远端块
    /// </summary>
    private static void ResolveDeadlock()
    {
        if (_buffer.IsEmpty) return;

        long furthestOffset = _buffer.Keys.Max();

        if (furthestOffset == _nextWriteOffset) return;

        if (_buffer.TryRemove(furthestOffset, out _))
        {
            _bufferSlots.Release();
            LogToConsole($"[Deadlock Breaker] Evicted block {furthestOffset} to free buffer slot.", ConsoleColor.Magenta);
        }
    }

    /// <summary>
    /// 生成下载队列，严格限制窗口大小在 Buffer 容量内，防止填充无用的远端块
    /// </summary>
    private static ConcurrentQueue<Chunk> GenerateChunksQueue(long startOffset)
    {
        var queue = new ConcurrentQueue<Chunk>();
        long current = startOffset;
        
        int windowSize = _maxBufferCount; 
        int count = 0;

        var existingKeys = new HashSet<long>(_buffer.Keys);

        while (current < _totalFileSize && count < windowSize)
        {
            if (!existingKeys.Contains(current))
            {
                long end = Math.Min(current + _blockSizeBytes - 1, _totalFileSize - 1);
                queue.Enqueue(new Chunk { Start = current, End = end });
            }
            current += _blockSizeBytes;
            count++;
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

        string etaStr = "--:--:--";
        if (speed > 0) 
        {
            try 
            { 
                TimeSpan eta = TimeSpan.FromSeconds(remainingBytes / speed);
                etaStr = eta.TotalDays >= 1 
                    ? $"{eta.Days}d {eta.Hours:D2}:{eta.Minutes:D2}:{eta.Seconds:D2}" 
                    : $"{eta:hh\\:mm\\:ss}";
            } 
            catch { }
        }

        int barWidth = 20;
        int filled = (int)(progressPct * barWidth);
        string bar = "[" + new string('=', filled) + ">" + new string(' ', Math.Max(0, barWidth - filled - 1)) + "]";
        if (filled >= barWidth) bar = "[" + new string('=', barWidth) + "]";

        string speedStr = $"{FormatSize((long)speed)}/s".PadRight(10);
        
        string extraInfo = "";
        // 如果卡顿则显示诊断信息
        if ((DateTime.Now - lastActivityTime).TotalSeconds > 5)
        {
            speedStr = "STALLED!".PadRight(10);
            extraInfo = $" | [{_diagStatus}]";
        }

        // 使用 padding 覆盖可能残留的旧字符
        string output = $"\r{bar} {progressPct:P1} | {FormatSize(_totalBytesWritten)} | {speedStr} | ETA: {etaStr}{extraInfo}";
        
        // 如果输出重定向了，就不要计算 padding，因为 WindowWidth 不可用
        if (Console.IsOutputRedirected)
        {
            Console.Write(output);
        }
        else
        {
            int padding = Console.WindowWidth - output.Length - 1;
            if (padding > 0) output += new string(' ', padding);
            Console.Write(output);
        }
    }

    /// <summary>
    /// 辅助方法：打印日志前先清除当前行，防止破坏进度条显示
    /// </summary>
    private static void LogToConsole(string msg, ConsoleColor color)
    {
        // 如果输出被重定向，直接WriteLine，不操作光标和颜色
        if (Console.IsOutputRedirected)
        {
            Console.WriteLine(msg);
            return;
        }

        // 先清除当前行
        Console.Write("\r" + new string(' ', Console.WindowWidth - 1) + "\r");
        var prev = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.WriteLine(msg);
        Console.ForegroundColor = prev;
    }

    private static string FormatSize(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1) { order++; len /= 1024; }
        return $"{len:0.00} {sizes[order]}";
    }

    private struct Chunk { public long Start; public long End; }

    // ======================================
    // 下载管理器
    // ======================================
    private class DownloadManager
    {
        private string _url;
        private int _threadCount;
        private CancellationTokenSource _cts;
        private HttpClient _client;
        private volatile bool _isRestarting = false;

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
                // 适度的连接寿命，平衡复用与端口轮换
                PooledConnectionLifetime = TimeSpan.FromMinutes(2),
                MaxConnectionsPerServer = _threadCount + 20,
                ConnectTimeout = TimeSpan.FromSeconds(10)
            };

            var client = new HttpClient(handler) { Timeout = TimeSpan.FromHours(24) };

            if (RobustDownloader._basicAuthHeader != null)
            {
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue(
                        "Basic",
                        RobustDownloader._basicAuthHeader.Substring(6));
            }

            return client;
        }

        public async Task StartAsync()
        {
            while (_totalBytesWritten < _totalFileSize)
            {
                _isRestarting = false;
                
                var chunksQueue = GenerateChunksQueue(_nextWriteOffset);

                if (chunksQueue.IsEmpty)
                {
                    await Task.Delay(500);
                    continue;
                }

                var activeTasks = new List<Task>();

                LogToConsole($"[DownloadManager] Starting loop. Pending Chunks: {chunksQueue.Count}", ConsoleColor.DarkGray);

                try
                {
                    while (!chunksQueue.IsEmpty && !_isRestarting)
                    {
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
                    LogToConsole("DownloadManager is resetting connection pool...", ConsoleColor.Yellow);
                    try { await Task.WhenAll(activeTasks); } catch { }
                    _cts.Dispose();
                    _cts = new CancellationTokenSource();
                    _client.Dispose();
                    _client = CreateHttpClient();
                    LogToConsole("Reset complete. Resuming download.", ConsoleColor.Yellow);
                }
            }
        }

        public void SoftRestart()
        {
            if (!_isRestarting)
            {
                _isRestarting = true;
                _cts.Cancel();
            }
        }
    }
}