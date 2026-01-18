using System.Diagnostics;
using System.Text;

// 1. 注册编码支持 (解决 GBK 乱码)
Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

Console.OutputEncoding = Encoding.UTF8;
Console.InputEncoding = Encoding.UTF8;
Console.Title = "FLAC 完美封装工具 (保留原标签)";

// 2. 获取 FFmpeg
string? ffmpegPath = GetFFmpegPath();
if (ffmpegPath is null)
{
    Log(ConsoleColor.Red, "错误: 未找到 ffmpeg.exe");
    Console.ReadKey();
    return;
}

if (args.Length == 0)
{
    Console.WriteLine("请将 .flac 文件拖放到此程序图标上...");
    Console.ReadKey();
    return;
}

int success = 0, fail = 0;

foreach (string path in args)
{
    if (!File.Exists(path)) continue;
    if (!Path.GetExtension(path.AsSpan()).Equals(".flac", StringComparison.OrdinalIgnoreCase)) continue;

    string dir = Path.GetDirectoryName(path)!;
    string nameNoExt = Path.GetFileNameWithoutExtension(path);
    string stem = Path.Combine(dir, nameNoExt);

    string jpgPath = stem + ".jpg";
    string pngPath = stem + ".png";
    string lrcPath = stem + ".lrc";

    // 封面是必须的
    string? coverPath = File.Exists(jpgPath) ? jpgPath : (File.Exists(pngPath) ? pngPath : null);
    if (coverPath is null)
    {
        Log(ConsoleColor.Yellow, $"[跳过] 无封面: {nameNoExt}");
        fail++;
        continue;
    }

    try
    {
        // 歌词是可选的
        string? targetLrc = File.Exists(lrcPath) ? lrcPath : null;
        ProcessFile(ffmpegPath, stem, path, coverPath, targetLrc);
        success++;
    }
    catch (Exception ex)
    {
        Log(ConsoleColor.Red, $"[失败] {nameNoExt}: {ex.Message}");
        fail++;
    }
}

Console.WriteLine($"\n处理完成: 成功 {success}, 失败 {fail}");
Console.WriteLine("按任意键退出...");
Console.ReadKey();

// ==========================================
//              核心逻辑函数
// ==========================================

void ProcessFile(string ffmpegExe, string stem, string flacPath, string coverPath, string? lrcPath)
{
    string outputFlac = stem + "_with_cover.flac";
    string fileName = Path.GetFileName(stem);
    Console.WriteLine($"处理中: {fileName}");

    // --- 步骤 1: 准备元数据 ---
    string finalMetadataContent = "";
    bool useMetadataPipe = false;

    if (lrcPath is not null)
    {
        Console.WriteLine("   -> 读取原文件标签...");
        // 1.1 获取原文件 Metadata (字符串)
        string originalMeta = GetOriginalMetadata(ffmpegExe, flacPath);
        
        // 1.2 读取歌词并格式化
        string lyricsBody = ReadAndFormatLyrics(lrcPath);

        // 1.3 合并 (原 Metadata + 换行 + 歌词块)
        // 注意：originalMeta 已经包含了 ;FFMETADATA1 头部
        finalMetadataContent = originalMeta + "\n" + lyricsBody;
        useMetadataPipe = true;
    }
    else
    {
        // 如果没有歌词文件，我们不需要通过管道注入 Metadata，
        // 直接让 FFmpeg 默认复制原文件 Metadata 即可。
        useMetadataPipe = false;
    }

    // --- 步骤 2: 构建 FFmpeg 命令 ---
    ProcessStartInfo psi = new()
    {
        FileName = ffmpegExe,
        UseShellExecute = false,
        CreateNoWindow = true,
        RedirectStandardInput = true,
        RedirectStandardError = true
    };

    psi.ArgumentList.Add("-hide_banner");
    psi.ArgumentList.Add("-loglevel");
    psi.ArgumentList.Add("error");
    psi.ArgumentList.Add("-y");

    // Input 0: 原音频
    psi.ArgumentList.Add("-i");
    psi.ArgumentList.Add(flacPath);

    // Input 1: Metadata 管道 (仅当有歌词时)
    if (useMetadataPipe)
    {
        psi.ArgumentList.Add("-f");
        psi.ArgumentList.Add("ffmetadata");
        psi.ArgumentList.Add("-i");
        psi.ArgumentList.Add("pipe:0");
    }

    // Input 1/2: 封面图片
    psi.ArgumentList.Add("-i");
    psi.ArgumentList.Add(coverPath);

    // 映射: 音频来自 Input 0
    psi.ArgumentList.Add("-map");
    psi.ArgumentList.Add("0:a");

    // 映射: 封面来自 图片流
    // 如果有 Metadata Pipe，图片是 Input 2；否则是 Input 1
    int coverIndex = useMetadataPipe ? 2 : 1;
    psi.ArgumentList.Add("-map");
    psi.ArgumentList.Add($"{coverIndex}:v");

    // 映射: 全局 Metadata
    if (useMetadataPipe)
    {
        // 如果使用了管道，明确指定使用管道(Input 1)作为 Metadata 来源
        // 因为管道里现在包含了“原标签+新歌词”的所有内容
        psi.ArgumentList.Add("-map_metadata");
        psi.ArgumentList.Add("1");
    }
    else
    {
        // 如果没歌词，默认就是 map_metadata 0 (复制原文件)，不需要额外写
    }

    psi.ArgumentList.Add("-c");
    psi.ArgumentList.Add("copy");
    psi.ArgumentList.Add("-disposition:v");
    psi.ArgumentList.Add("attached_pic");
    psi.ArgumentList.Add(outputFlac);

    // --- 步骤 3: 执行 FFmpeg ---
    using Process proc = Process.Start(psi) ?? throw new Exception("启动失败");

    if (useMetadataPipe)
    {
        try
        {
            // 写入合并后的完整 Metadata
            byte[] bytes = Encoding.UTF8.GetBytes(finalMetadataContent);
            proc.StandardInput.BaseStream.Write(bytes);
            proc.StandardInput.BaseStream.Flush();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   写入 Metadata 出错: {ex.Message}");
        }
        finally
        {
            proc.StandardInput.Close();
        }
    }

    string stderr = proc.StandardError.ReadToEnd();
    proc.WaitForExit();

    if (proc.ExitCode != 0) throw new Exception(stderr);
    
    Log(ConsoleColor.Green, "   -> 成功");
}

// 提取原文件的 Metadata 字符串
string GetOriginalMetadata(string ffmpegExe, string flacPath)
{
    // 命令: ffmpeg -i input.flac -f ffmetadata pipe:1
    ProcessStartInfo psi = new()
    {
        FileName = ffmpegExe,
        UseShellExecute = false,
        CreateNoWindow = true,
        RedirectStandardOutput = true, // 捕获标准输出
        RedirectStandardError = true   // 忽略日志
    };
    
    psi.ArgumentList.Add("-hide_banner");
    psi.ArgumentList.Add("-i");
    psi.ArgumentList.Add(flacPath);
    psi.ArgumentList.Add("-f");
    psi.ArgumentList.Add("ffmetadata");
    psi.ArgumentList.Add("pipe:1"); // 输出到 StdOut

    using Process proc = Process.Start(psi) ?? throw new Exception("无法读取原文件标签");
    
    // 读取输出 (Metadata 内容)
    string meta = proc.StandardOutput.ReadToEnd();
    proc.WaitForExit();
    
    return meta;
}

// 仅生成 LYRICS=xxxx 部分，不带头部
string ReadAndFormatLyrics(string path)
{
    // 1. 智能读取 (防乱码)
    string text = ReadLrcSmart(path);
    
    StringBuilder sb = new StringBuilder();
    sb.Append("LYRICS="); // Key

    string[] lines = text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);

    for (int i = 0; i < lines.Length; i++)
    {
        string line = lines[i];
        if (string.IsNullOrWhiteSpace(line)) continue;

        // 转义特殊字符
        StringBuilder escaped = new StringBuilder();
        foreach (char c in line)
        {
            if (c == '=' || c == ';' || c == '#' || c == '\\') escaped.Append('\\');
            escaped.Append(c);
        }
        sb.Append(escaped);

        // 处理换行 (行尾加 反斜杠+换行)
        if (i < lines.Length - 1)
        {
            sb.Append("\\\n");
        }
    }
    
    return sb.ToString();
}

string ReadLrcSmart(string path)
{
    string text = File.ReadAllText(path, Encoding.UTF8);
    // 检测黑钻问号
    if (text.Contains('\uFFFD'))
    {
        try { return File.ReadAllText(path, Encoding.GetEncoding(54936)); }
        catch { 
            try { return File.ReadAllText(path, Encoding.GetEncoding(936)); }
            catch { return File.ReadAllText(path, Encoding.Default); }
        }
    }
    return text;
}

string? GetFFmpegPath()
{
    string local = Path.Combine(AppContext.BaseDirectory, "ffmpeg.exe");
    if (File.Exists(local)) return local;
    try { Process.Start(new ProcessStartInfo("ffmpeg", "-version") { CreateNoWindow = true, UseShellExecute = false })?.WaitForExit(); return "ffmpeg"; }
    catch { return null; }
}

void Log(ConsoleColor color, string msg)
{
    Console.ForegroundColor = color;
    Console.WriteLine(msg);
    Console.ResetColor();
}