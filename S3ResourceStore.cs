using Agilix.Shared;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace S3Performance
{
    enum EncryptionType
    {
        None,
        SSE_S3,
        SSE_KMS,    // not supported yet!
        SSE_C,
    }
    class S3StoreInfo : IDisposable
    {
        public S3StoreInfo(IAmazonS3 client, int? sliceId, string bucket, string prefix, EncryptionType encryption)
        {
            Client = client;
            SliceId = sliceId;
            Bucket = bucket;
            Prefix = (prefix ?? string.Empty).Replace(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).TrimEnd(Path.AltDirectorySeparatorChar) + Path.AltDirectorySeparatorChar;
            Encryption = encryption;
        }

        public int? SliceId { get; private set; }
        public IAmazonS3 Client { get; private set; }
        public string Bucket { get; private set; }
        public string Prefix { get; private set; }
        public EncryptionType Encryption { get; private set; }

        public string BuildKey(string physicalPath) { return Prefix + physicalPath; }

        public void Dispose()
        {
            Client.Dispose();
        }
    }

    public class S3ResourceStore
    {
        private static byte[] _EncryptionKey = CreateEncryptionKey();
        public static string EncryptionKey = Convert.ToBase64String(_EncryptionKey);
        public static string EncryptionKeyMD5 = Convert.ToBase64String(ComputeMD5Hash(_EncryptionKey));
        private static byte[] CreateEncryptionKey()
        {
            byte[] encryptionKey = new byte[32];
            Buffer.BlockCopy(Guid.NewGuid().ToByteArray(), 0, encryptionKey, 0, 16);
            Buffer.BlockCopy(Guid.NewGuid().ToByteArray(), 0, encryptionKey, 16, 16);
            return encryptionKey;
        }
        private static byte[] ComputeMD5Hash(byte[] buffer)
        {
            using (MD5 hash = MD5.Create())
            {
                hash.ComputeHash(buffer);
                return hash.Hash;
            }
        }

        private static readonly ConcurrentDictionary<string, S3StoreInfo> sS3Stores = new ConcurrentDictionary<string, S3StoreInfo>();

        private readonly S3StoreInfo fStore;

        private static S3StoreInfo OpenS3Store(string path)
        {
            string[] parts = path.Split('/');
            if (parts.Length < 3) throw new ArgumentException("The specified S3 resource path (" + path + ") is invalid!");
            EncryptionType encryptionType;
            switch (parts[0].ToLowerInvariant())
            {
                case "s3:":
                    encryptionType = EncryptionType.None;
                    break;
                case "s3e:":
                    encryptionType = EncryptionType.SSE_S3;
                    break;
                case "s3k:":
                    encryptionType = EncryptionType.SSE_KMS;    // not supported yet!
                    break;
                case "s3x:":
                    encryptionType = EncryptionType.SSE_C;
                    break;
                default:
                    throw new ArgumentException("The specified resource path (" + path + ") is not a valid S3 resource path!");
            }
            string bucket = parts[2];
            string prefix = String.Join("/", parts, 3, parts.Length - 3) + "/";

            if (!String.IsNullOrEmpty(parts[1]) || String.IsNullOrEmpty(bucket) || String.IsNullOrEmpty(prefix) || prefix.StartsWith("/"))
            {
                throw new ArgumentException("The specified resource path (" + path + ") is not a valid S3 resource path!");
            }

            int? sliceId = SystemExtensions.TryParseInt(parts[parts.Length - 1]);

            return new S3StoreInfo(new AmazonS3Client(), sliceId, bucket, prefix, encryptionType);
        }

        internal S3ResourceStore(string s3Path)
        {
            // get the store (only make one store for each unique set of parameters)
            fStore = OpenS3Store(s3Path);
        }

        public static byte[] ComputeMD5Hash(Stream stream)
        {
            using (MD5 hash = MD5.Create())
            {
                stream.Position = 0;
                byte[] data = hash.ComputeHash(stream);
                stream.Position = 0;
                return data;
            }
        }

        /// <summary>
        /// Opens a stream to access a resource in read-mode.
        /// </summary>
        /// <param name="physicalPath">The physical path of the resource.</param>
        /// <returns>A <see cref="Stream"/> with read access to the resource.</returns>
        /// <remarks>
        /// <para><b>physicalPath</b> is in a format proprietary to a particular resource store.</para>
        /// </remarks>
        public Stream OpenResource(string physicalPath)
        {
            // this is an S3 resource file, so build the S3ResourceStream to manage it
            return new S3ResourceStream(fStore, fStore.Bucket, fStore.BuildKey(physicalPath));
        }

        /// <summary>
        /// Deletes an existing resource.
        /// </summary>
        /// <param name="physicalPath">The physical path of the resource to delete.</param>
        /// <remarks>
        /// <para>No exceptions are thrown regardless of whether a deletion was successful. If
        /// the implementation needs to record an error of some sort, it should use the logging system.
        /// </remarks>
        public void DeleteResource(string physicalPath)
        {
            DeleteObjectRequest request = new DeleteObjectRequest
            {
                BucketName = fStore.Bucket,
                Key = fStore.BuildKey(physicalPath)
            };
            fStore.Client.DeleteObject(request);
        }

        public Stopwatch WriteResource(Stream source, byte[] hash, string physicalPath)
        {
            // we always need the hash for S3, so compute it now if we didn't already do that
            if (hash == null)
            {
                hash = ComputeMD5Hash(source);
            }

            Stopwatch timer = Stopwatch.StartNew();

            PutObjectRequest request = new PutObjectRequest
            {
                BucketName = fStore.Bucket,
                Key = fStore.BuildKey(physicalPath),
                InputStream = source,
                AutoCloseStream = false,
                Timeout = TimeSpan.FromSeconds(600),
                ReadWriteTimeout = TimeSpan.FromSeconds(600),
            };

            switch (fStore.Encryption)
            {
                case EncryptionType.None:
                    break;
                case EncryptionType.SSE_C:
                    request.ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256;
                    request.ServerSideEncryptionCustomerProvidedKey = EncryptionKey;
                    request.ServerSideEncryptionCustomerProvidedKeyMD5 = EncryptionKeyMD5;
                    break;
                case EncryptionType.SSE_S3:
                    request.ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS;
                    break;
                //case EncryptionType.SSE_KMS:
                //    request.ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256;
                //    request.ServerSideEncryptionKeyManagementServiceKeyId = ServerSideEncryptionMethod.AES256;
                //    break;
                default: throw new InvalidOperationException();
            }

            try
            {
                PutObjectResponse response = fStore.Client.PutObject(request);
            }
            catch (AmazonS3Exception ex)
            {
                Console.WriteLine("ID2: " + ex.AmazonId2 + " failed with code " + ex.ErrorCode);
                throw;
            }
            timer.Stop();

            return timer;
        }

        private class S3ResourceStream : Stream
        {
            private const int BufferBytes = 16 * 1024;
            private readonly S3ResourceFile fFile;
            private S3ResourceStreamBuffer fBuffer;
            private long fCursor;

            public S3ResourceStream(S3StoreInfo store, string bucketName, string physicalPath)
            {
                fFile = new S3ResourceFile(store, bucketName, physicalPath);
                fCursor = 0;
                // start with an empty buffer
                fBuffer = fFile.GetBufferWithRetry(0, BufferBytes);
            }

#if DEBUG
            private string fStackAtConstruction = new StackTrace().ToString();
            ~S3ResourceStream()
            {
                System.Diagnostics.Debug.Fail("S3ResourceStream not disposed.  Stack at construction: " + fStackAtConstruction);
            }
#endif

            override protected void Dispose(bool disposing)
            {
                if (disposing)
                {
                    if (fFile != null)
                    {
                        fFile.Dispose();
                    }
                    GC.SuppressFinalize(this);
                }
            }

            public override bool CanRead
            {
                get { return true; }
            }

            public override bool CanSeek
            {
                get { return true; }
            }

            public override bool CanWrite
            {
                get { return false; }
            }

            public override void Flush()
            {
            }

            public override long Length
            {
                get { return fFile.TotalBytes; }
            }

            public override long Position
            {
                get
                {
                    return fCursor;
                }
                set
                {
                    Seek(value, SeekOrigin.Begin);
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                int totalBytesRead = 0;
                // how many bytes will we read?
                int bytesToRead = Math.Min(count, (int)Math.Min(Int32.MaxValue, fFile.TotalBytes - fCursor));
                do
                {
                    // attempt to read from the current buffer?
                    int bytesRead = fBuffer.ReadAt(fCursor, buffer, offset, bytesToRead);
                    // adjust pointers
                    bytesToRead -= bytesRead;
                    fCursor += bytesRead;
                    offset += bytesRead;
                    totalBytesRead += bytesRead;
                    // do we need to refill the buffer?
                    if (bytesToRead > 0 && (bytesRead == 0 || fCursor >= fBuffer.Offset + fBuffer.ValidBufferBytes))
                    {
                        fBuffer = fFile.GetBufferWithRetry(fCursor, BufferBytes);
                    }
                } while (bytesToRead > 0);
                return totalBytesRead;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                long position;
                switch (origin)
                {
                    case SeekOrigin.Current:
                        position = fCursor + offset;
                        break;
                    case SeekOrigin.End:
                        position = fFile.TotalBytes + offset;
                        break;
                    default:
                        position = offset;
                        break;
                }
                if (position < 0) throw new ArgumentException("The specified offset would put the cursor before the beginning of the file!", "offset");
                fCursor = position;
                return fCursor;
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }
        }
        private class S3ResourceFile : IDisposable
        {
            private readonly S3StoreInfo fStore;
            private readonly string fBucketName;
            private readonly string fPhysicalPath;
            private GetObjectResponse fResponse;
            private long fReponseCursor;
            private long fTotalBytes;
            private SemaphoreSlim fStreamLock = new SemaphoreSlim(1);


            /// <summary>
            /// Constructs an object which will read chunks of a file on S3.
            /// </summary>
            /// <param name="store">The S3 store info.</param>
            /// <param name="bucketName">The S3 bucket name.</param>
            /// <param name="physicalPath">The physical path within the bucket.</param>
            public S3ResourceFile(S3StoreInfo store
                , string bucketName, string physicalPath)
            {
                fStore = store;
                fBucketName = bucketName;
                fPhysicalPath = physicalPath;
            }

#if DEBUG
            private string fStackAtConstruction = new StackTrace().ToString();
            ~S3ResourceFile()
            {
                System.Diagnostics.Debug.Fail("S3ResourceFile not disposed.  Stack at construction: " + fStackAtConstruction);
            }
#endif
            private void OpenResponse(long startOffset = 0)
            {
                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = fBucketName,
                    Key = fPhysicalPath,
                };
#if false
                GetPreSignedUrlRequest psur = new GetPreSignedUrlRequest
                {
                    BucketName = fBucketName,
                    Key = fPhysicalPath,
                    Verb = HttpVerb.GET,
                    Expires = DateTime.Now.AddMinutes(15),
                };
#endif
                switch (fStore.Encryption)
                {
                    case EncryptionType.None:
                        break;
                    case EncryptionType.SSE_C:
                        request.ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256;
                        request.ServerSideEncryptionCustomerProvidedKey = EncryptionKey;
                        request.ServerSideEncryptionCustomerProvidedKeyMD5 = EncryptionKeyMD5;
#if false
psur.Headers["x-amz-server-side​-encryption​-customer-algorithm"] = "AES256";
psur.Headers["x-amz-server-side​-encryption​-customer-key"] = EncryptionKey;
psur.Headers["x-amz-server-side​-encryption​-customer-key-MD5"] = EncryptionKeyMD5;
#endif
                        break;
                    case EncryptionType.SSE_S3:
                        break;
                    default: throw new InvalidOperationException();
                }
#if false
string url = fStore.Client.GetPreSignedURL(psur);
Console.WriteLine(url);
#endif
                long requestedStartOffset = 0;
                // are we requesting data at a spot other than the beginning of the file and we know the file size?
                if (startOffset != 0 && fTotalBytes != 0)
                {
                    // tell S3 the byte range we want (from the specified offset to the end of the file)
                    requestedStartOffset = startOffset;
                    request.ByteRange = new ByteRange(startOffset, fTotalBytes - 1);
                }
                Stopwatch timer = Stopwatch.StartNew();
                using (fResponse) { }
                fResponse = fStore.Client.GetObject(request);
                fTotalBytes = requestedStartOffset + fResponse.ContentLength;
                fReponseCursor = requestedStartOffset;
            }

            /// <summary>
            /// Gets the total number of bytes in the S3 file.
            /// </summary>
            public long TotalBytes { get { return fTotalBytes; } }

            /// <summary>
            /// Gets a <see cref="S3ResourceBuffer"/> with the specified range of the file data.
            /// </summary>
            /// <param name="offset">The offset within the file whose data is to be read.</param>
            /// <param name="bytes">The number of bytes in the file that should be buffered.</param>
            /// <returns>The <see cref="S3ResourceStreamBuffer"/> for that range of the file.</returns>
            public S3ResourceStreamBuffer GetBufferWithRetry(long offset, int bytes)
            {
                byte[] buffer = new byte[bytes];
                try
                {
                    // first call?
                    if (fResponse == null) OpenResponse();
                    if (offset >= fTotalBytes) return new S3ResourceStreamBuffer(buffer, offset, 0);
                    int bytesRead = GetBuffer(offset, buffer, 0, bytes);
                    return new S3ResourceStreamBuffer(buffer, offset, bytesRead);
                }
                catch
                {
                    // reopen the S3 object and try one more time (if we throw here, let it fail!)
                    OpenResponse();
                    int bytesRead = GetBuffer(offset, buffer, 0, bytes);
                    return new S3ResourceStreamBuffer(buffer, offset, bytesRead);
                }
            }
            private int GetBuffer(long fileOffset, byte[] buffer, int bufferOffset, int bytes)
            {
                fStreamLock.Wait();
                try
                {
                    Stopwatch timer = Stopwatch.StartNew();
                    int bytesRead = 0;
                    // not at the right position?
                    if (fReponseCursor != fileOffset)
                    {
                        OpenResponse(fileOffset);
                    }
                    System.Diagnostics.Debug.Assert(fReponseCursor == fileOffset);
                    bytesRead = fResponse.ResponseStream.Read(buffer, 0, buffer.Length);
                    fReponseCursor += bytesRead;
                    System.Diagnostics.Debug.Assert(bytesRead <= bytes);
                    return bytesRead;
                }
                finally
                {
                    fStreamLock.Release();
                }
            }

            public void Dispose()
            {
                if (fResponse != null)
                {
                    fResponse.Dispose();
                    fResponse = null;
                }
                GC.SuppressFinalize(this);
            }
        }
        private class S3ResourceStreamBuffer
        {
            private long fOffset;
            private byte[] fBuffer;
            private int fValidBufferBytes;

            public S3ResourceStreamBuffer(byte[] buffer, long offset, int validBufferBytes)
            {
                fBuffer = buffer;
                fOffset = offset;
                fValidBufferBytes = validBufferBytes;
            }
            public long Offset { get { return fOffset; } }
            public int ValidBufferBytes { get { return fValidBufferBytes; } }

            public int ReadAt(long offset, byte[] buffer, int bufferOffset, int bytes)
            {
                // offset out of range?
                if (offset + bytes < fOffset || offset > fOffset + fValidBufferBytes) return 0;
                // figure out what bytes to copy and copy them
                int sourceOffset = (int)Math.Min((long)Int32.MaxValue, offset - fOffset);
                int maxSourceBytesToCopy = (int)(fOffset + fValidBufferBytes - offset);
                int bytesToCopy = Math.Min(bytes, maxSourceBytesToCopy);
                Buffer.BlockCopy(fBuffer, sourceOffset, buffer, bufferOffset, bytesToCopy);
                return bytesToCopy;
            }
        }
    }
}
