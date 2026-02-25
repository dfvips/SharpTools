using System;
using System.IO;
using System.Text;
using System.Buffers.Binary;
using System.Linq;

class Program
{
    static int Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        if (args.Length == 0)
        {
            ProcessAllCrc64InCurrentDir();
            return 0;
        }

        if (args.Length != 1)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  crc64ecma file.mp4");
            Console.WriteLine("  crc64ecma file.mp4.crc64");
            return 1;
        }

        string input = args[0];

        if (input.EndsWith(".crc64", StringComparison.OrdinalIgnoreCase))
            VerifyCrcList(input);
        else
            ComputeSingle(input);

        return 0;
    }

    // ================= 无参数：处理当前目录所有 crc64 =================

    static void ProcessAllCrc64InCurrentDir()
    {
        var files = Directory
            .GetFiles(Directory.GetCurrentDirectory(), "*.crc64")
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (files.Length == 0)
        {
            WriteColored("No .crc64 files found in current directory.", ConsoleColor.DarkGray);
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
            return;
        }

        foreach (var file in files)
        {
            Console.WriteLine();
            WriteColored($"Processing {NameOnly(file)}", ConsoleColor.Cyan);
            VerifyCrcList(file);
        }
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    // ================= 单文件计算 =================

    static void ComputeSingle(string file)
    {
        if (!File.Exists(file))
        {
            WriteColored($"{NameOnly(file)}  FILE NOT FOUND", ConsoleColor.DarkGray);
            return;
        }

        using var fs = File.OpenRead(file);
        ulong crc = Crc64Ecma.Compute(fs, p => DrawProgress(p, file));

        Console.WriteLine();
        WriteColored($"{crc} *{NameOnly(file)}", ConsoleColor.Green);
    }

    // ================= crc64 文件校验 =================

    static void VerifyCrcList(string crcFile)
    {
        if (!File.Exists(crcFile))
        {
            WriteColored($"{NameOnly(crcFile)}  FILE NOT FOUND", ConsoleColor.DarkGray);
            return;
        }

        var lines = File.ReadAllLines(crcFile);
        int total = lines.Length;
        int index = 0;

        foreach (var line in lines)
        {
            index++;

            if (string.IsNullOrWhiteSpace(line))
                continue;

            string file;
            ulong expected;

            var legacyParts = line.Split("====", StringSplitOptions.RemoveEmptyEntries);
            if (legacyParts.Length == 2)
            {
                file = legacyParts[0].Trim();
                expected = ulong.Parse(legacyParts[1].Trim());
            }
            else
            {
                int spaceIdx = line.IndexOf(' ');
                if (spaceIdx <= 0) continue;

                string num = line.Substring(0, spaceIdx).Trim();
                string name = line.Substring(spaceIdx + 1).Trim();
                if (name.StartsWith("*")) name = name.Substring(1).Trim();

                expected = ulong.Parse(num);
                file = name;
            }

            if (!File.Exists(file))
            {
                DrawProgress(index * 100 / total, file);
                Console.WriteLine();
                WriteColored($"{NameOnly(file)}  FILE NOT FOUND", ConsoleColor.DarkGray);
                continue;
            }

            using var fs = File.OpenRead(file);
            ulong actual = Crc64Ecma.Compute(fs, p =>
            {
                long overall = ((index - 1) * 100 + p) / total;
                DrawProgress(overall, file);
            });

            Console.WriteLine();

            if (actual == expected)
                WriteColored($"{NameOnly(file)}  OK  ({actual})", ConsoleColor.Green);
            else
                WriteColored(
                    $"{NameOnly(file)}  MISMATCH  ({actual} != {expected})",
                    ConsoleColor.Red
                );
        }
    }

    // ================= 工具方法 =================

    static void DrawProgress(long percent, string path)
    {
        const int width = 28;
        int filled = (int)(percent * width / 100);

        Console.CursorLeft = 0;
        Console.Write('[');
        Console.Write(new string('█', filled));
        Console.Write(new string('-', width - filled));
        Console.Write($"] {percent,3}%  {NameOnly(path)}");
    }

    static void WriteColored(string text, ConsoleColor color)
    {
        var old = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.WriteLine(text);
        Console.ForegroundColor = old;
    }

    static string NameOnly(string path)
        => Path.GetFileName(path);
}

// ================= CRC64-ECMA（Go 官方 slicing-by-8 等价实现） =================

sealed class Crc64Ecma
{
    private const ulong Polynomial = 0xC96C5795D7870F42UL;
    private static readonly ulong[,] Table = BuildTable();
    private ulong _crc;

    private static ulong[,] BuildTable()
    {
        var table = new ulong[8, 256];

        for (ulong i = 0; i < 256; i++)
        {
            ulong crc = i;
            for (int j = 0; j < 8; j++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ Polynomial : crc >> 1;
            table[0, i] = crc;
        }

        for (int slice = 1; slice < 8; slice++)
        {
            for (int i = 0; i < 256; i++)
            {
                ulong crc = table[slice - 1, i];
                table[slice, i] = table[0, crc & 0xFF] ^ (crc >> 8);
            }
        }

        return table;
    }

    public void Update(ReadOnlySpan<byte> data)
    {
        ulong crc = ~_crc;
        int i = 0;

        while (i + 8 <= data.Length)
        {
            ulong block = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(i, 8));
            crc ^= block;

            crc =
                Table[7, crc & 0xFF] ^
                Table[6, (crc >> 8) & 0xFF] ^
                Table[5, (crc >> 16) & 0xFF] ^
                Table[4, (crc >> 24) & 0xFF] ^
                Table[3, (crc >> 32) & 0xFF] ^
                Table[2, (crc >> 40) & 0xFF] ^
                Table[1, (crc >> 48) & 0xFF] ^
                Table[0, crc >> 56];

            i += 8;
        }

        for (; i < data.Length; i++)
            crc = Table[0, (byte)(crc ^ data[i])] ^ (crc >> 8);

        _crc = ~crc;
    }

    public static ulong Compute(Stream stream, Action<long>? progress)
    {
        var c = new Crc64Ecma();
        byte[] buffer = new byte[1024 * 1024];

        long total = stream.Length;
        long read = 0;

        int n;
        while ((n = stream.Read(buffer, 0, buffer.Length)) > 0)
        {
            c.Update(buffer.AsSpan(0, n));
            read += n;
            progress?.Invoke(read * 100 / total);
        }

        return c._crc;
    }
}
