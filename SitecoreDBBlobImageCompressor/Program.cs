using System;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.IO;

namespace SitecoreDBBlobImageCompressor
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var dryRun = false;
                bool.TryParse(args[0], out dryRun);
                CompressBlobTable(dryRun);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static SqlConnection GetConnectionToSitecoreMaster()
        {

            var connection =
                System.Configuration.ConfigurationManager.ConnectionStrings["master"].ConnectionString;

            return new SqlConnection(connection);

        }

        private static void CompressBlobTable(bool dryRun)
        {
            if (dryRun)
            {
                Console.WriteLine("Dry Run Enabled");
            }

            using (var connection = GetConnectionToSitecoreMaster())
            {
                var command = new SqlCommand("select Id, Data from BlobsCompressed", connection);
                // Set the UPDATE command and parameters.  

                var count = 0;
                var compressedCount = 0;
                var skippedCount = 0;
                using (var dataTable = new DataTable())
                {

                    SqlDataAdapter da = new SqlDataAdapter(command);
                    da.SelectCommand.CommandTimeout = 3600;
                    
                    var cmdBuilder = new SqlCommandBuilder(da);

                    var updateCommand = new SqlCommand(
                        "UPDATE BlobsCompressed SET "
                        + "Data=@Data WHERE Id=@Id;",
                        connection);

                    da.UpdateCommand = updateCommand;
                    da.UpdateCommand.CommandTimeout = 7200; // 60 mins
                    da.UpdateCommand.Parameters.Add("@Data",
                        SqlDbType.Image, 0, "Data");

                    da.UpdateCommand.Parameters.Add("@Id",
                        SqlDbType.UniqueIdentifier, 36, "Id");

                    da.UpdateCommand.UpdatedRowSource = UpdateRowSource.None;

                    da.Fill(dataTable);

                    try
                    {
                        foreach (DataRow row in dataTable.Rows)
                        {
                            count++;
                            var blobId = row["Id"].ToString();
                            var img = (byte[]) row["Data"];

                            Console.WriteLine($"Compressing BlobId: {blobId}");
                            MemoryStream str = new MemoryStream();
                            str.Write(img, 0, img.Length);
                            try
                            {
                                Bitmap bit = new Bitmap(str);
                                var compressedBlob =
                                    CompressJpegImage(bit,
                                        75); //75 seems to be the sweet spot

                                if (compressedBlob == null)
                                {
                                    skippedCount++;
                                    continue;
                                }

                                if (!dryRun)
                                {
                                    row["Data"] = compressedBlob;
                                }
                                compressedCount++;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"BlobId: {blobId} is not an image. Skipping....");
                                // Not an image... skipping...
                                Console.WriteLine(ex);
                                skippedCount++;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception occured");
                        Console.WriteLine(ex);
                    }

                    Console.WriteLine($"Rows processed: {count}");
                    Console.WriteLine($"Images compressed: {compressedCount}");
                    Console.WriteLine($"Rows skipped: {skippedCount}");

                    Console.WriteLine($"Updating data...");
                    da.UpdateBatchSize = 0;
                    da.Update(dataTable);
                    
                }
            }
        }

        public static byte[] ReadFully(Stream input)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                input.CopyTo(ms);
                return ms.ToArray();
            }
        }


        private static Byte[] CompressJpegImage(Image image, int compressionPercentage)
        {
            try
            {
                var bmp = (Bitmap) image;
                using (var mozJpeg = new MozJpeg.MozJpeg())
                {
                    var rawJpeg = mozJpeg.Encode(bmp, compressionPercentage);
                    return rawJpeg;
                    //return new MemoryStream(rawJpeg);
                }
            }
            catch (Exception)
            {
                //Console.WriteLine(ex);
                return null;
            }
        }

    }
}
