using Agilix.Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace S3Performance
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine(String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0} usage: {0} <target_location> <files> <bytes_per_file>", Environment.GetCommandLineArgs()[0]));
                Console.WriteLine("  <target_location>: a target folder path of the form: s3://my_bucket_name/myobjectkey");
                Console.WriteLine("    s3:// uses no encryption");
                Console.WriteLine("    s3e:// uses SSE-S3 encryption");
                Console.WriteLine("    s3k:// uses SSE-KMS encryption");
                Console.WriteLine("    s3x:// uses SSE-C encryption");
                Console.WriteLine("  <files> is the number of files to read and then writer");
                Console.WriteLine("  <bytes_per_file> is the average number of bytes to write per file");
                Console.WriteLine("    the actual number of bytes will vary randomly between half this number and 50% more than this number");
                Console.WriteLine();
                return;
            }
            string targetLocation = args[0];
            S3ResourceStore store = new S3ResourceStore(targetLocation);
            Random r = new Random();
            int count = Int32.Parse(args[1]);
            long size = Int32.Parse(args[2]);
            long totalMicroseconds = 0;
            long totalBytes = 0;
            HashSet<string> filenames = new HashSet<string>();
            for (int loop = 0; loop < count; ++loop)
            {
                int randomPart = r.Next((int)size);
                long bytesToWrite = size - (size / 2) + randomPart;
                totalBytes += bytesToWrite;

                using (Stream source = new RandomStream(loop, bytesToWrite))
                {
                    Guid guid = Guid.NewGuid();
                    string filename = guid.ToString("N");
                    filenames.Add(filename);
                    Stopwatch timer = store.WriteResource(source, null, filename);
                    long microseconds = timer.GetElapsedMicroseconds();
                    totalMicroseconds += microseconds;
                    Console.WriteLine("Write Latency: " + microseconds + "us");
                }
            }
            long averageWriteLatencyMicroseconds = totalMicroseconds / count;
            long averageWriteBytesPerSecond = totalBytes * 1000 / totalMicroseconds;
            totalMicroseconds = 0;
            totalBytes = 0;
            byte[] buffer = new byte[8192];
            foreach (string filename in filenames)
            {
                Stopwatch timer = new Stopwatch();
                timer.Start();
                using (Stream s = store.OpenResource(filename))
                {
                    timer.Stop();
                    int bytesRead = 1;
                    int ops = 0;
                    while (bytesRead > 0)
                    {
                        timer.Start();
                        bytesRead = s.Read(buffer, 0, buffer.Length);
                        totalBytes += bytesRead;
                        timer.Stop();
                        ++ops;
                    }
                    long microseconds = timer.GetElapsedMicroseconds();
                    totalMicroseconds += microseconds;
                    Console.WriteLine("Read Latency: " + microseconds + "us");
                }
            }
            long averageReadLatencyMicroseconds = totalMicroseconds / count;
            long averageReadBytesPerSecond = totalBytes * 1000 / totalMicroseconds;
            Console.WriteLine("Average Write Latency: " + averageWriteLatencyMicroseconds + "us");
            Console.WriteLine("Average Read Latency: " + averageReadLatencyMicroseconds + "us");
            Console.WriteLine("Average Write KB/sec: " + averageWriteBytesPerSecond);
            Console.WriteLine("Average Read KB/sec: " + averageReadBytesPerSecond);
        }
    }
}
