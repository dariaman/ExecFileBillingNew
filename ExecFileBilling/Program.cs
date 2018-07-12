using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class Program
    {
        static readonly string constring = ConfigurationManager.AppSettings["DefaultDB"];
        static readonly string FileResult = ConfigurationManager.AppSettings["DirResult"];
        static readonly string FileBackup = ConfigurationManager.AppSettings["BackupResult"];

        static string FileBilling = ConfigurationManager.AppSettings["FileBilling"];
        static string BillingBackup = ConfigurationManager.AppSettings["BillingBackup"];
        static DateTime TglNow = DateTime.Now;
        static readonly DateTime Tgl = DateTime.Now.Date;


        static void Main(string[] args)
        {
            /*
             * jika argument "exec 7" => maka 7 adalah id di tabel FileNextProcess
             * jika argument "upload 7" => maka 7 adalah id di tabel FileNextProcess
             */

            //args = new string[] { "exec", "11" };
            //args = new string[] { "upload", "2" };
            //args = new string[] { "remove", "13" };

            if (args.Count() < 1)
            {
                Console.WriteLine("Parameter tidak terdefenisi...");
                Console.WriteLine("Aplication exit...");
                Thread.Sleep(5000);
                return;
            }
            if (args[0] == "upload")
            {
                if (args.Count() < 2)
                {
                    Console.WriteLine("Parameter Kurang...");
                    Console.WriteLine("Aplication exit...");
                    Thread.Sleep(5000);
                    return;
                }
                FileResultModel FileUpload;
                if (args[1] != "")
                {
                    if (!int.TryParse(args[1], out int idx))
                    {
                        Console.WriteLine("Parameter Salah");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }

                    FileUpload = GetUploadFile(idx);
                    if (FileUpload.FileName == null)
                    {
                        Console.WriteLine("FileName Kosong");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }
                    try
                    {
                        //jika file tidak bisa dibaca
                        if (!File.OpenRead(FileResult + FileUpload.FileName).CanRead)
                        {
                            Console.WriteLine("File tidak bisa di proses");
                            Console.WriteLine("Aplication exit...");
                            Thread.Sleep(5000);
                            return;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }

                    // Jika data sudah pernah diinsert atas file tersebut -> exit
                    if (CekFileInsert(idx, FileUpload.stageTable))
                    {
                        Console.WriteLine("Data Sudah Pernah insert . . . ");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }

                    ProsesHapusDataStaging(idx, FileUpload.stageTable);

                    List<DataUploadModel> DataUpload = new List<DataUploadModel>();
                    if (idx == 1 || idx == 2) DataUpload = BacaFileBCA_CC(FileUpload.FileName); //BCA cc
                    else if (idx == 3) DataUpload = BacaFileMandiri_CC(FileUpload.FileName); //mandiri cc
                    else if (idx == 4 || idx == 5) DataUpload = BacaFileMega_CC(FileUpload.FileName); //mega cc
                    else if (idx == 6) DataUpload = BacaFileBNI_CC(FileUpload.FileName);  // BNI cc
                    else if (idx >= 7 && idx <= 10) DataUpload = BacaFilCIMB_CC(FileUpload.FileName); // cimb cc
                    else if (idx == 11) DataUpload = BacaFileBCA_AC(FileUpload.FileName); // bca ac
                    else if (idx == 12) DataUpload = BacaFileMandiri_AC(FileUpload.FileName); // mandiri ac
                    else if (idx == 13)
                    {
                        DataUpload = BacaFileVA_daily(FileUpload.FileName); // va daily

                        // Jika baca va daily data kosong, maka baca format va realtime
                        if (DataUpload.Count < 1) DataUpload = BacaFileVA_realtime(FileUpload.FileName); // va realtime
                    }
                    //else if (idx == 14) DataUpload = BacaFileVA_realtime(FileUpload.FileName); // va realtime

                    InsertTableStaging(DataUpload, FileUpload.stageTable, FileUpload.FileName);
                    MapingData(idx, FileUpload.stageTable);

                    // check billing berdasarkan paid_date untuk va
                    if (idx == 13) UpdateBilling_VA_Paid(idx, FileUpload.stageTable);

                    // hitung jumlah data upload (reject,approve)
                    HitungJumlahDataUpload(idx, FileUpload.stageTable);
                }
            }
            else if (args[0] == "exec")
            {
                if (args.Count() == 1) ExceuteDataUpload();
                else if (args.Count() > 1)
                {
                    if (!int.TryParse(args[1], out int idx))
                    {
                        Console.WriteLine("Parameter Salah");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }
                    ExceuteDataUpload(idx);
                }
            }
            else if (args[0] == "remove")
            {
                if (args.Count() < 2)
                {
                    Console.WriteLine("Parameter Kurang...");
                    Console.WriteLine("Aplication exit...");
                    Thread.Sleep(5000);
                    return;
                }
                if (args[1] != "")
                {
                    if (!int.TryParse(args[1], out int idx))
                    {
                        Console.WriteLine("Parameter Salah");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }

                    FileResultModel FileUpload = GetUploadFile(idx);
                    try
                    {
                        ProsesHapusDataStaging(idx, FileUpload.stageTable);

                        // hapus file fisik
                        RemoveFileUploadResult(FileUpload);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }

                    // hitung jumlah data upload (reject,approve), setelah di remove data 
                    HitungJumlahDataUpload(idx, FileUpload.stageTable);
                }
            }
            else
            {
                Console.WriteLine("Nothing . . . ");
                Thread.Sleep(5000);
                return;
            }

            Console.WriteLine();
            Console.WriteLine("F I N I S H . . . ");
            Thread.Sleep(5000);
        }

        public static Boolean CekFileInsert(int id, string tablename)
        {
            var Isdata = false;

            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT 1 
                        FROM `FileNextProcess` fp
                        INNER JOIN " + tablename + @" up ON up.`FileName`=fp.`FileName`
                        WHERE fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL AND fp.`id`=@idx
                        LIMIT 1;", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });

            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read()) { Isdata = true; }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("CekFileInsert() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Isdata;
        }

        public static Boolean CekOtherFileInsert(int id, string tablename)
        {
            /*
             * jika file upload ada 2 seperti bca CC, dan CIMB CC
             * jika di upload file approve, maka yang di cek file reject dan sebaliknya
             */
            var Isdata = false;
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT 1
                                    FROM FileNextProcess fp
                                    INNER JOIN FileNextProcess fx ON fx.`id_billing_download`=fp.`id_billing_download` AND fx.`id`<>@idx
                                    INNER JOIN " + tablename + @" up ON up.`FileName`=fx.`FileName`
                                    WHERE fp.`id`=@idx LIMIT 1; ", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });

            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read()) { Isdata = true; }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("CekOtherFileInsert() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Isdata;
        }

        public static FileResultModel GetUploadFile(int id)
        {
            FileResultModel Fileproses = new FileResultModel();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT fp.* ,bs.`file_download`
                                    FROM `FileNextProcess` fp
                                    LEFT JOIN `billing_download_summary` bs ON bs.`id`=fp.`id_billing_download`
                                    WHERE fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL
                                    AND fp.id=@idx;", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        Fileproses = new FileResultModel()
                        {
                            Id = Convert.ToInt32(rd["id"]),
                            trancode = rd["trancode"].ToString(),
                            FileName = rd["FileName"].ToString(),
                            stageTable = rd["stageTable"].ToString(),
                            FileBilling = rd["file_download"].ToString(),
                            tglProses = Convert.ToDateTime(rd["tglProses"]),
                            source = rd["source"].ToString(),
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            bankid = Convert.ToInt32(rd["bankid"]),
                            id_billing_download = Convert.ToInt32(rd["id_billing_download"]),
                            deskripsi = rd["deskripsi"].ToString()
                        };
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("genFile() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Fileproses;
        }

        public static List<FileResultModel> GenFile()
        {
            List<FileResultModel> Fileproses = new List<FileResultModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT fp.* ,bs.`file_download`
                                    FROM `FileNextProcess` fp
                                    LEFT JOIN `billing_download_summary` bs ON bs.`id`=fp.`id_billing_download`
                                    WHERE fp.`FileName` IS NOT NULL 
                                    AND fp.`tglProses` IS NOT NULL
                                    AND fp.`tglProses` = CURDATE(); ", con)
            {
                CommandType = CommandType.Text
            };
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        Fileproses.Add(new FileResultModel()
                        {
                            Id = Convert.ToInt32(rd["id"]),
                            trancode = rd["trancode"].ToString(),
                            FileName = rd["FileName"].ToString(),
                            stageTable = rd["stageTable"].ToString(),
                            FileBilling = rd["file_download"].ToString(),
                            tglProses = Convert.ToDateTime(rd["tglProses"]),
                            source = rd["source"].ToString(),
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            bankid = Convert.ToInt32(rd["bankid"]),
                            id_billing_download = Convert.ToInt32(rd["id_billing_download"]),
                            deskripsi = rd["deskripsi"].ToString(),
                            tglSkrg = TglNow
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("genFile() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Fileproses;
        }

        public static FileResultModel GenFile(int id)
        {
            FileResultModel Fileproses = null;
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT fp.* ,bs.`file_download`
                                    FROM `FileNextProcess` fp
                                    LEFT JOIN `billing_download_summary` bs ON bs.`id`=fp.`id_billing_download`
                                    WHERE fp.`FileName` IS NOT NULL
                                    AND fp.`tglProses` IS NOT NULL
                                    AND fp.`tglProses` = CURDATE() AND fp.id=@idx;", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        Fileproses = new FileResultModel()
                        {
                            Id = Convert.ToInt32(rd["id"]),
                            trancode = rd["trancode"].ToString(),
                            FileName = rd["FileName"].ToString(),
                            stageTable = rd["stageTable"].ToString(),
                            FileBilling = rd["file_download"].ToString(),
                            tglProses = Convert.ToDateTime(rd["tglProses"]),
                            source = rd["source"].ToString(),
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            bankid = Convert.ToInt32(rd["bankid"]),
                            id_billing_download = Convert.ToInt32(rd["id_billing_download"]),
                            deskripsi = rd["deskripsi"].ToString(),
                            tglSkrg = TglNow
                        };
                    }
                }
            }
            catch (Exception ex) { throw new Exception("GenFile(int id) : " + ex.Message); }
            finally { con.CloseAsync(); }

            return Fileproses;
        }

        public static List<FileResultModel> GenFileCC()
        {
            List<FileResultModel> Fileproses = new List<FileResultModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT fp.* ,bs.`file_download`
                                    FROM `FileNextProcess` fp
                                    LEFT JOIN `billing_download_summary` bs ON bs.`id`=fp.`id_billing_download`
                                    WHERE fp.`FileName` IS NOT NULL fp.`source`='CC' 
                                    AND fp.`tglProses` IS NOT NULL
                                    AND fp.`tglProses` = CURDATE(); ", con)
            {
                CommandType = CommandType.Text
            };
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        Fileproses.Add(new FileResultModel()
                        {
                            Id = Convert.ToInt32(rd["id"]),
                            trancode = rd["trancode"].ToString(),
                            FileName = rd["FileName"].ToString(),
                            stageTable = rd["stageTable"].ToString(),
                            FileBilling = rd["file_download"].ToString(),
                            tglProses = Convert.ToDateTime(rd["tglProses"]),
                            source = rd["source"].ToString(),
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            bankid = Convert.ToInt32(rd["bankid"]),
                            id_billing_download = Convert.ToInt32(rd["id_billing_download"]),
                            deskripsi = rd["deskripsi"].ToString(),
                            tglSkrg = TglNow
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("genFile() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Fileproses;
        }

        public static List<DataSubmitModel> PoolDataProsesApprove(int id, string tableName)
        {
            //IsSukses 0=Reject, 1=Approve
            Console.Write("Pooling data Approve ... " + tableName + " ... ");
            List<DataSubmitModel> DataProses = new List<DataSubmitModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT u.* 
                                    FROM `FileNextProcess` fp
                                    INNER JOIN " + tableName + @" u ON u.`FileName`=fp.`FileName`
                                    WHERE fp.`id`=@idx AND fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL
                                    AND fp.`tglProses`=CURDATE() AND u.`IsExec`=0 AND u.`IsSukses`=1 AND u.BillCode='B' 
                                    ORDER BY u.`PolisId`,u.`Amount`;")
            {
                CommandType = CommandType.Text,
                Connection = con
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });

            try
            {
                con.Open();
                DateTime? tgl_paid = null;
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        if (DateTime.TryParse(rd["TranDate"].ToString().Trim(), out DateTime tgl_temp)) tgl_paid = tgl_temp; else tgl_paid = null;
                        DataProses.Add(new DataSubmitModel()
                        {
                            id = Convert.ToInt32(rd["id"]),
                            PolisNo = rd["PolisNo"].ToString(),
                            Amount = Convert.ToDecimal(rd["Amount"]),
                            ApprovalCode = rd["ApprovalCode"].ToString(),
                            Deskripsi = rd["Deskripsi"].ToString(),
                            AccNo = rd["AccNo"].ToString(),
                            AccName = rd["AccName"].ToString(),
                            IsSukses = Convert.ToBoolean(rd["IsSukses"]),
                            PolisId = rd["PolisId"].ToString(),
                            BillCode = rd["BillCode"].ToString(),
                            paid_date = tgl_paid,
                            GroupRejectMapping = rd["RejectGroupID"].ToString(),
                            //BillingID = (rd["BillingID"].ToString() == string.Empty) ? null : rd["BillingID"].ToString(),
                            //BillStatus = rd["BillStatus"].ToString(),
                            //PolisStatus = rd["PolisStatus"].ToString(),
                            //PremiAmount = Convert.ToDecimal(rd["PremiAmount"]),
                            //CashlessFeeAmount = Convert.ToDecimal(rd["CashlessFeeAmount"]),
                            //TotalAmount = Convert.ToDecimal(rd["TotalAmount"])
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("PoolDataProsesApprove(B) : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsExec`=0 AND u.`IsSukses`=1 AND u.BillCode<>'B' AND u.`BillingID` IS NOT NULL;", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Clear();
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        DataProses.Add(new DataSubmitModel()
                        {
                            id = Convert.ToInt32(rd["id"]),
                            PolisNo = rd["PolisNo"].ToString(),
                            Amount = Convert.ToDecimal(rd["Amount"]),
                            ApprovalCode = rd["ApprovalCode"].ToString(),
                            Deskripsi = rd["Deskripsi"].ToString(),
                            AccNo = rd["AccNo"].ToString(),
                            AccName = rd["AccName"].ToString(),
                            IsSukses = Convert.ToBoolean(rd["IsSukses"]),
                            BillingID = (rd["BillingID"].ToString() == string.Empty) ? null : rd["BillingID"].ToString(),
                            BillCode = rd["BillCode"].ToString(),
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("PoolDataProsesApprove(X) : " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }

            if (DataProses.Count < 1) Console.Write("=> Kosong");
            else Console.Write(DataProses.Count.ToString());
            Console.WriteLine();
            Thread.Sleep(5000);
            return DataProses;
        }

        public static void RemoveFileUploadResult(FileResultModel Fileproses)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"UPDATE `FileNextProcess` SET `FileName`=NULL,`tglProses`=NULL WHERE `id`=@id;", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = Fileproses.Id });
            try
            {
                FileInfo Filex = new FileInfo(FileResult + Fileproses.FileName);
                if (Filex.Exists) Filex.MoveTo(FileBackup + Fileproses.FileName);

                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("removeFile() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void RemoveFileBilling(FileResultModel Fileproses)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand();
            try
            {

                cmd = new MySqlCommand(@"UpdateBillSum")
                {
                    CommandType = CommandType.StoredProcedure,
                    Connection = con
                };
                cmd.Connection.Open();
                cmd.ExecuteNonQuery();
                cmd.Connection.Close();

                cmd = new MySqlCommand(@"SELECT b.`TotalAmountDWD`+b.`TotalCountDWD` FROM `billing_download_summary` b WHERE b.`id`=@idd;")
                {
                    CommandType = CommandType.Text,
                    Connection = con

                };
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new MySqlParameter("@idd", MySqlDbType.Int32) { Value = Fileproses.id_billing_download });

                cmd.Connection.Open();
                var data = cmd.ExecuteScalar();
                cmd.Connection.Close();

                if (!Decimal.TryParse(data.ToString(), out decimal itemData)) return;
                if (itemData > 0) return;

                cmd = new MySqlCommand(@"UPDATE `billing_download_summary` bd SET bd.`file_download`=NULL WHERE bd.`id`=@id;")
                {
                    CommandType = CommandType.Text,
                    Connection = con
                };
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = Fileproses.id_billing_download });
                cmd.Connection.Open();
                cmd.ExecuteNonQuery();

                FileInfo Filex = new FileInfo(FileBilling.Trim() + Fileproses.FileBilling.Trim());
                if (Filex.Exists) Filex.MoveTo(BillingBackup.Trim() + Fileproses.FileBilling.Trim() + Regex.Replace(Guid.NewGuid().ToString(), "[^0-9a-zA-Z]", "").Substring(0, 8));

            }
            catch (Exception ex) { throw new Exception("RemoveFileBilling() : " + ex.Message); }
            finally
            {
                if (cmd.Connection.State == ConnectionState.Open) cmd.Connection.CloseAsync();
            }
        }

        public static void InsertTableStaging(List<DataUploadModel> DataUpload, string tableName, string FileName)
        {
            String sqlStart = @"INSERT INTO " + tableName + "(PolisNo,Amount,TranDate,ApprovalCode,Deskripsi,AccNo,AccName,IsSukses,FileName) values ";
            string sql = "";
            int i = 0, j = 1;
            foreach (DataUploadModel item in DataUpload)
            {
                if (item == null) continue;
                i++;
                sql = sql + string.Format(@"('{0}',{1},{2},NULLIF('{3}',''),NULLIF('{4}',''),NULLIF('{5}',''),'{6}',{7},'{8}'),",
                    item.PolisNo,
                    item.Amount,
                    item.TglPaid == null ? "NULL" : string.Concat("'", item.TglPaid.Value.ToString("yyyy-MM-dd HH:mm:ss"), "'"),
                    item.ApprovalCode,
                    item.Deskripsi==null ? "" : item.Deskripsi.ToString().Replace("'", "*"),
                    item.AccNo,
                    item.AccName == null ? "" : item.AccName.ToString().Replace("'","*"),
                    item.IsSukses,
                    FileName);
                // eksekusi per 100 data
                if (i == 100)
                {
                    Console.WriteLine(j.ToString() + ". insert 100");
                    j++;
                    ExecQueryAsync(sqlStart + sql.TrimEnd(','));
                    sql = "";
                    i = 0;
                }
            }
            //eksekusi sisanya 
            if (i > 0) ExecQueryAsync(sqlStart + sql.TrimEnd(','));
        }

        public static void ExecQueryAsync(string query)
        {
            using (MySqlConnection con = new MySqlConnection(constring))
            {
                MySqlCommand cmd = new MySqlCommand(query, con);
                cmd.Parameters.Clear();
                cmd.CommandType = CommandType.Text;
                try
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    throw new Exception("ExecQueryAsync() : " + ex.Message);
                }
                finally
                {
                    con.CloseAsync();
                }
            }
        }

        public static List<DataUploadModel> BacaFileBCA_CC(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses)))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var panjang = line.Length;
                    if (panjang < 171) continue;

                    if (!Decimal.TryParse(line.Substring(54, 9), out decimal tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        PolisNo = line.Substring(9, 25).Trim(),
                        AccNo = line.Substring(34, 16).Trim(),
                        AccName = line.Substring(65, 26).Trim(),
                        Amount = tmp1,
                        ApprovalCode = (line.Substring(line.Length - 2) == "00")
                                        ? line.Substring(line.Length - 8).Substring(0, 6).Trim()
                                        : line.Substring(line.Length - 2),
                        Deskripsi = null,
                        IsSukses = (line.Substring(line.Length - 2) == "00") ? true : false,
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileMandiri_CC(string Fileproses)
        {
            RegexOptions options = RegexOptions.None;
            Regex regex = new Regex("[ ]{2,}", options);
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                ExcelPackage xl = new ExcelPackage(fs);
                ExcelWorkbook wb = xl.Workbook;

                if ((wb.Worksheets[1] == null) || (wb.Worksheets[2] == null)) return null;
                // Sheet Approve (sheet 1) 
                ExcelWorksheet ws = wb.Worksheets[1];
                ExcelCellAddress startCell = ws.Dimension.Start;
                ExcelCellAddress endCell = ws.Dimension.End;

                Decimal tmp1;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    // cek cell yang digunakan tidak null
                    if ((ws.Cells[row, 2].Value == null) || (ws.Cells[row, 3].Value == null) ||
                        (ws.Cells[row, 4].Value == null) || (ws.Cells[row, 7].Value == null) ||
                        (ws.Cells[row, 6].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        AccName = ws.Cells[row, 2].Value.ToString().Trim().Replace("'", "\\'"),
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 4].Value.ToString().Trim(),
                        PolisNo = ws.Cells[row, 6].Value.ToString().Trim(),
                        AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = true
                    });
                }

                // Sheet Reject (sheet 2) 
                ws = wb.Worksheets[2];
                startCell = ws.Dimension.Start;
                endCell = ws.Dimension.End;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    if ((ws.Cells[row, 2].Value == null) || (ws.Cells[row, 3].Value == null) ||
                        (ws.Cells[row, 4].Value == null) || (ws.Cells[row, 7].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        AccName = ws.Cells[row, 2].Value.ToString().Trim().Replace("'", "\\'"),
                        Amount = tmp1,
                        PolisNo = ws.Cells[row, 4].Value.ToString().Trim(),
                        ApprovalCode = (ws.Cells[row, 5].Value ?? "").ToString().Trim(),
                        Deskripsi = regex.Replace(ws.Cells[row, 6].Value.ToString().Trim().Replace("-", " ").Replace("'", "\\'"), " "),
                        AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = false
                    });
                }
                fs.Close();
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileMega_CC(string Fileproses)
        {
            RegexOptions options = RegexOptions.None;
            Regex regex = new Regex("[ ]{2,}", options);
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                ExcelPackage xl = new ExcelPackage(fs);
                ExcelWorkbook wb = xl.Workbook;

                if ((wb.Worksheets[1] == null) || (wb.Worksheets[2] == null)) return null;
                // Sheet Approve (sheet 1) 
                ExcelWorksheet ws = wb.Worksheets[1];
                ExcelCellAddress startCell = ws.Dimension.Start;
                ExcelCellAddress endCell = ws.Dimension.End;

                Decimal tmp1;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    // cek cell yang digunakan tidak null
                    if ((ws.Cells[row, 1].Value == null) || (ws.Cells[row, 2].Value == null) ||
                        (ws.Cells[row, 3].Value == null) || (ws.Cells[row, 4].Value == null) ||
                        (ws.Cells[row, 5].Value == null) || (ws.Cells[row, 6].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    var temp = ws.Cells[row, 2].Value.ToString().Trim();
                    dataUpload.Add(new DataUploadModel()
                    {
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 5].Value.ToString().Trim(),
                        PolisNo = temp.Split('-').Last().Trim(),
                        IsSukses = true
                    });
                }

                // Sheet Reject (sheet 2) 
                ws = wb.Worksheets[2];
                startCell = ws.Dimension.Start;
                endCell = ws.Dimension.End;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    if ((ws.Cells[row, 1].Value == null) || (ws.Cells[row, 2].Value == null) ||
                        (ws.Cells[row, 3].Value == null) || (ws.Cells[row, 4].Value == null) ||
                        (ws.Cells[row, 5].Value == null) || (ws.Cells[row, 6].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    var temp = ws.Cells[row, 2].Value.ToString().Trim();
                    dataUpload.Add(new DataUploadModel()
                    {
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 5].Value.ToString().Trim(),
                        Deskripsi = regex.Replace(ws.Cells[row, 6].Value.ToString().Trim().Replace("-", " ").Replace("'", "\\'"), " "),
                        PolisNo = temp.Split('-').Last().Trim(),
                        IsSukses = false
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileBNI_CC(string Fileproses)
        {
            RegexOptions options = RegexOptions.None;
            Regex regex = new Regex("[ ]{2,}", options);
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                ExcelPackage xl = new ExcelPackage(fs);
                ExcelWorkbook wb = xl.Workbook;

                if (wb.Worksheets[1] == null) return null;
                // Sheet Approve (sheet 1) 
                ExcelWorksheet ws = wb.Worksheets[1];
                ExcelCellAddress startCell = ws.Dimension.Start;
                ExcelCellAddress endCell = ws.Dimension.End;

                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    // cek cell yang digunakan tidak null
                    if ((ws.Cells[row, 1].Value == null) || (ws.Cells[row, 4].Value == null) ||
                        (ws.Cells[row, 7].Value == null) || (ws.Cells[row, 8].Value == null) ||
                        (ws.Cells[row, 9].Value == null) || (ws.Cells[row, 10].Value == null)) continue;

                    // no urut
                    if (!Decimal.TryParse(ws.Cells[row, 1].Value.ToString().Trim(), out decimal tmp1)) continue;
                    if (!Decimal.TryParse(ws.Cells[row, 8].Value.ToString().Trim(), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 9].Value.ToString().Trim(),
                        PolisNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        AccNo = ws.Cells[row, 4].Value.ToString().Trim(),
                        AccName = ws.Cells[row, 5].Value.ToString().Trim().Replace("'", "\\'"),
                        Deskripsi = regex.Replace(ws.Cells[row, 10].Value.ToString().Trim().Replace("-", " ").Replace("'", "\\'"), " "),
                        IsSukses = (ws.Cells[row, 9].Value.ToString().Trim() == "") ? false : true
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFilCIMB_CC(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses)))
            {
                string line;
                int no_urut;
                Int64 polis_file;
                decimal amount_file;
                int pj;
                string ket;
                bool status_sukses = false;

                while ((line = reader.ReadLine()) != null)
                {
                    var panjang = line.Length;
                    if (panjang < 92) continue;

                    if (!int.TryParse(line.Substring(0, 6), out no_urut)) continue; // no urut harus angka
                    if (!Int64.TryParse(line.Substring(6, 16), out polis_file)) continue; // no polis harus angka
                    if (!Decimal.TryParse(line.Substring(42, 8), out amount_file)) continue; // amount harus deceimal
                    var desk = line.Substring(85, panjang - 85);

                    string appCode = "";
                    if (desk.Trim() == "APPROVE") status_sukses = true;
                    else
                    {
                        appCode = desk.Substring(7, 2);
                        pj = desk.Length;
                        ket = desk.Substring(10, pj - 10);
                        desk = ket;
                        status_sukses = false;
                    }

                    dataUpload.Add(new DataUploadModel()
                    {
                        PolisNo = polis_file.ToString(),
                        AccNo = line.Substring(22, 16).Trim(),
                        //AccName = line.Substring(65, 26).Trim(),
                        Amount = amount_file,
                        ApprovalCode = appCode,
                        Deskripsi = desk,
                        IsSukses = status_sukses,
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileBCA_AC(string Fileproses)
        {
            RegexOptions options = RegexOptions.None;
            Regex regex = new Regex("[ ]{2,}", options);
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses)))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var panjang = line.Length;
                    if (panjang < 205) continue;

                    if (!Decimal.TryParse(line.Substring(74, 18).Trim(), out decimal tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        PolisNo = line.Substring(92, 15).Trim(),
                        AccNo = line.Substring(37, 11).Trim(),
                        AccName = line.Substring(48, 26).Trim().Replace("'", "\\'"),
                        Amount = tmp1,
                        ApprovalCode = line.Substring(129, 9).Trim(),
                        Deskripsi = line.Substring(138, 51).Trim(),
                        IsSukses = (line.Substring(129, 9).Trim().ToLower() == "berhasil") ? true : false
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileMandiri_AC(string Fileproses)
        {
            RegexOptions options = RegexOptions.None;
            Regex regex = new Regex("[ ]{2,}", options);
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses)))
            {
                string line;
                Boolean status;
                while ((line = reader.ReadLine()) != null)
                {
                    var panjang = line.Length;
                    if (panjang < 720) continue;

                    if (!Decimal.TryParse(line.Substring(634, 40).Trim(), out decimal tmp1)) continue;
                    status = (line.Substring(674, 46).Trim().ToLower() == "success") ? true : false;
                    var acc = line.Substring(306, 244).Trim().Split('/');
                    var NoAcc = (acc.Length >= 2) ? acc[0] : null;
                    var nameAcc = line.Substring(306 + NoAcc.Length + 1, 244 - NoAcc.Length - 1).Replace("(IDR)", string.Empty);
                    dataUpload.Add(new DataUploadModel()
                    {
                        PolisNo = line.Substring(590, 40).Trim(),
                        AccNo = NoAcc.Trim(),
                        AccName = nameAcc.Trim(),
                        Amount = tmp1,
                        ApprovalCode = line.Substring(674, 46).Trim(),
                        Deskripsi = status ? line.Substring(720, panjang - 720).Trim().Replace("'", "\'") : null,
                        IsSukses = status
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileVA_realtime(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses)))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var panjang = line.Length;
                    if (panjang < 133) continue;

                    if (!int.TryParse(line.Substring(1, 5).Trim(), out int i)) continue; // cek no urut
                    if (!Decimal.TryParse(line.Substring(52, 19).Trim(), out decimal tmp1)) continue; // cek amount
                    if (!DateTime.TryParseExact(line.Substring(73, 18).Trim(), "dd/MM/yy  HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime tgl_Paid)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        PolisNo = line.Substring(8, 20).Trim(),
                        AccName = line.Substring(28, 18).Trim(),
                        Amount = tmp1,
                        Deskripsi = line.Substring(100, 33).Trim(),
                        TglPaid = tgl_Paid,
                        IsSukses = true
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileVA_daily(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses)))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var panjang = line.Length;
                    if (panjang < 195) continue;

                    if (!int.TryParse(line.Substring(0, 5).Trim(), out int i)) continue; // cek no urut
                    if (!Decimal.TryParse(line.Substring(112, 22).Trim(), out decimal tmp1)) continue; // cek amount
                    if (!DateTime.TryParseExact(line.Substring(136, 19).Trim(), "dd/MM/yyyy HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime tgl_Paid)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        PolisNo = line.Substring(11, 19).Trim(),
                        AccName = line.Substring(45, 31).Trim(),
                        Amount = tmp1,
                        Deskripsi = line.Substring(158, 37).Trim(),
                        TglPaid = tgl_Paid,
                        IsSukses = true
                    });
                }
            }
            return dataUpload;
        }

        public static void ProsesHapusDataStaging(int idx, string TableName)
        {
            if (idx == 1 || idx == 2 || (idx >= 7 && idx <= 10))
            {
                /*
                 * untuk data yang 2 file (1 file approve dan 1 file reject)
                 * contoh : jika yg di upload bca approve, cek apakah bca reject sudah di upload ?
                 * jika bca reject belum di upload maka langsung delete All data tabel staging
                 */
                if (CekOtherFileInsert(idx, TableName)) KosongkanSetengahTabel(idx, TableName);
                else KosongkanTabel(TableName);
            }
            else KosongkanTabel(TableName);
        }

        public static void KosongkanTabel(string TableName)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"DELETE FROM " + TableName + ";ALTER TABLE " + TableName + " AUTO_INCREMENT=1;", con);
            cmd.Parameters.Clear();
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("KosongkanTabel() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void KosongkanSetengahTabel(int idx, string TableName)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"DELETE up
                                    FROM " + TableName + @" up
                                    INNER JOIN `FileNextProcess` fp ON up.`FileName`=fp.`FileName`
                                    WHERE fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL AND fp.`id`=@id ;", con);
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.VarChar) { Value = idx });
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("KosongkanSetengahTabel() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void MapingData(int idx, string tableName)
        {
            Console.WriteLine("Mapping data  ...");
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;

            cmd = new MySqlCommand(@"
UPDATE " + tableName + @" up
INNER JOIN `policy_billing` pb ON pb.`policy_no`=up.`PolisNo`
" + ((idx == 2) ? "LEFT JOIN `reason_maping_group` rp ON rp.`RejectCode`=up.`ApprovalCode`" : "LEFT JOIN `reason_maping_group` rp ON rp.`ReajectReason`=up.`Deskripsi`") + @" AND up.`IsSukses`=0 
	SET up.`PolisId`=pb.`policy_Id`,up.`BillCode`='B',up.`RejectGroupID`=rp.`GroupRejectMappingID` " + (idx == 2 ? ",up.`Deskripsi`=rp.`ReajectReason`" : "") + @"
WHERE up.`IsExec`=0 AND LEFT(up.`PolisNo`,1) NOT IN ('A','X');

UPDATE " + tableName + @" up
INNER JOIN `billing_others` bo ON bo.`BillingID`=up.`PolisNo`
" + ((idx == 2) ? "LEFT JOIN `reason_maping_group` rp ON rp.`RejectCode`=up.`ApprovalCode`" : "LEFT JOIN `reason_maping_group` rp ON rp.`ReajectReason`=up.`Deskripsi`") + @" AND up.`IsSukses`=0
	SET up.`BillingID`=up.`PolisNo`,up.`BillCode`='A',up.`RejectGroupID`=rp.`GroupRejectMappingID` " + (idx == 2 ? ",up.`Deskripsi`=rp.`ReajectReason`" : "") + @"
WHERE up.`IsExec`=0 AND LEFT(up.`PolisNo`,1) ='A';

UPDATE " + tableName + @" up
INNER JOIN `quote_billing` q ON q.`quote_id`=SUBSTRING_INDEX(up.`PolisNo`,'X',-1)
" + ((idx == 2) ? "LEFT JOIN `reason_maping_group` rp ON rp.`RejectCode`=up.`ApprovalCode`" : "LEFT JOIN `reason_maping_group` rp ON rp.`ReajectReason`=up.`Deskripsi`") + @" AND up.`IsSukses`=0
	SET up.`BillingID`=q.`quote_id`,up.`BillCode`='Q',up.`RejectGroupID`=rp.`GroupRejectMappingID` " + (idx == 2 ? ",up.`Deskripsi`=rp.`ReajectReason`" : "") + @"
WHERE up.`IsExec`=0 AND LEFT(up.`PolisNo`,1) ='X';
                ", con);
            cmd.Parameters.Clear();
            cmd.CommandType = CommandType.Text;

            try
            {
                cmd.Connection.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("MapingDataApprove() : " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void HitungJumlahDataUpload(int idx, string TableName)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT COUNT(1) INTO @app FROM " + TableName + @" up WHERE up.`IsSukses`;
                                    SELECT COUNT(1) INTO @rjt FROM " + TableName + @" up WHERE NOT up.`IsSukses`;

                                    UPDATE `upload_sum` us
                                    INNER JOIN `FileNextProcess` fp ON us.`id`=fp.`id_upload_sum`
	                                    SET us.`count_approve`=@app, us.`count_reject`=@rjt, us.`total_upload`=@app+@rjt
                                    WHERE fp.`id`=@idx;

                                    SELECT COUNT(1) INTO @jlh_data_file
                                    FROM `FileNextProcess` fp
                                    INNER JOIN " + TableName + @" up ON up.`FileName`=fp.`FileName`
                                    WHERE fp.`id`=@idx;

                                    UPDATE `FileNextProcess` fp SET fp.total_data=@jlh_data_file WHERE fp.`id`=@idx;", con);
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.VarChar) { Value = idx });
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("HitungJumlahDataUpload() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void SubmitApproveTransaction(string tableName, List<DataSubmitModel> DataProses, FileResultModel DataHeader)
        {
            int i = 0;
            Console.WriteLine();
            foreach (DataSubmitModel item in DataProses)
            {
                i++;
                try
                {
                    Console.Write(String.Format("{0} ", i));

                    if (item.BillCode == "B") RecurringApprove(item, DataHeader);
                    else if (item.BillCode == "A" && DataHeader.source.Trim().ToUpper() != "VA") BillOtherApprove(item, DataHeader);
                    else if (item.BillCode == "Q") QuoteApprove(item, DataHeader);
                    Console.WriteLine(String.Format("PolisNo {0} ", item.PolisNo));
                }
                catch (Exception ex) { throw new Exception("SubmitApproveTransaction() : " + ex.Message); }
            }
        }

        public static void RecurringApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();

            string billingID = "";
            decimal CashlessFeeAmount = 0;
            int freq_payment = 1;

            try
            {
                GetBillingUnpaid(ref billingID, ref freq_payment, ref CashlessFeeAmount, DataProses.PolisId);
                if (billingID == "") throw new Exception("Billing Kosong....");

                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                // Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT `transaction_bank`(`File_Backup`,`TranCode`,`TranDate`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
values (@FileBackup,@TranCode,@TranDate,@PolicyId,@BillingID,@BillAmount,NULLIF(@ApprovalCode,''),NULLIF(@Description,''),NULLIF(@accNo,''),@accName);
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@TranDate", MySqlDbType.DateTime) { Value = DataProses.paid_date });
                cmd.Parameters.Add(new MySqlParameter("@PolicyId", MySqlDbType.VarChar) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = billingID });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.VarChar) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.VarChar) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalar().ToString();

                // Insert Receipt
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt`(`receipt_date`,`receipt_policy_id`, `receipt_fund_type_id`, `receipt_transaction_code`, `receipt_amount`,
`receipt_source`, `receipt_status`, `receipt_payment_date_time`, `receipt_seq`, `bank_acc_id`, `due_date_pre`,`acquirer_bank_id`,`freq_payment`,`created_by`)
SELECT @tgl,up.`PolisId`,0,'RP',up.`Amount`-b.`cashless_fee_amount`,@source,'P',@tgl,b.`recurring_seq`,@bankAccId,b.`due_dt_pre`,@bankid,@freq,2000
FROM " + DataHeader.stageTable + @" up
LEFT JOIN `billing` b ON b.`BillingID`=@Billid
WHERE up.`id`=@Id;
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@Id", MySqlDbType.Int32) { Value = DataProses.id });
                cmd.Parameters.Add(new MySqlParameter("@Billid", MySqlDbType.Int32) { Value = billingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@source", MySqlDbType.VarChar) { Value = DataHeader.source });
                cmd.Parameters.Add(new MySqlParameter("@bankAccId", MySqlDbType.Int32) { Value = DataHeader.bankid_receipt });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@freq", MySqlDbType.Int32) { Value = freq_payment });
                DataProses.receiptID = cmd.ExecuteScalar().ToString();

                // Insert Polis Note Receipt
                string pesan = "RECEIPT INPUT RP (" + DataHeader.source + ")";
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"insert into `prod_life21`.policy_note(policy_id, date_tran, message, staff_id) 
                                    SELECT @PolisId, @tgl,@pesan,2000;
                                    SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@PolisId", MySqlDbType.Int32) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@pesan", MySqlDbType.VarChar) { Value = pesan });
                DataProses.PolisNoteReceiptID = cmd.ExecuteScalar().ToString();

                // Insert Receipt Other
                if (CashlessFeeAmount > 0)
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt_other`(`receipt_date`,`policy_id`,`receipt_type_id`,`receipt_amount`,`receipt_source`,`receipt_payment_date`,`receipt_seq`,`bank_acc_id`,`acquirer_bank_id`,`receipt_id`,`created_by`)
SELECT @tgl,b.`policy_id`,3,b.`cashless_fee_amount`,@source,@tgl,b.`recurring_seq`,@bankAccId,@bankid,@receipt_id,2000
FROM " + DataHeader.stageTable + @" up
INNER JOIN `billing` b ON b.`BillingID`=@Billid
WHERE up.`id`=@Id;
SELECT LAST_INSERT_ID();";
                    cmd.Parameters.Add(new MySqlParameter("@Id", MySqlDbType.Int32) { Value = DataProses.id });
                    cmd.Parameters.Add(new MySqlParameter("@Billid", MySqlDbType.Int32) { Value = billingID });
                    cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                    cmd.Parameters.Add(new MySqlParameter("@source", MySqlDbType.VarChar) { Value = DataHeader.source });
                    cmd.Parameters.Add(new MySqlParameter("@bankAccId", MySqlDbType.Int32) { Value = DataHeader.bankid_receipt });
                    cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                    cmd.Parameters.Add(new MySqlParameter("@receipt_id", MySqlDbType.Int32) { Value = DataProses.receiptID });
                    DataProses.receiptOtherID = cmd.ExecuteScalar().ToString();

                    // Insert Polis Note Receipt Other
                    pesan = "RECEIPT INPUT Pengguna Cashless (" + DataHeader.source + ")";
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"insert into `prod_life21`.policy_note(policy_id, date_tran, message, staff_id) 
                                    SELECT @PolisId, @tgl,@pesan,2000;
                                    SELECT LAST_INSERT_ID();";
                    cmd.Parameters.Add(new MySqlParameter("@PolisId", MySqlDbType.Int32) { Value = DataProses.PolisId });
                    cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                    cmd.Parameters.Add(new MySqlParameter("@pesan", MySqlDbType.VarChar) { Value = pesan.ToUpper() });
                    DataProses.PolisNoteReceiptOtherID = cmd.ExecuteScalar().ToString();
                }

                // start CC
                if (DataHeader.source.Trim().ToUpper() == "CC")
                {
                    // Insert CC Transaction
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"
INSERT INTO `prod_life21`.`policy_cc_transaction`(`policy_id`,`transaction_dt`,`transaction_type`,`recurring_seq`,
`count_times`,`currency`,`total_amount`,`due_date_pre`,`due_date_pre_period`,`acquirer_bank_id`,
`cc_no`,`cc_name`,`status_id`,`remark`,`receipt_id`,`receipt_other_id`,`created_dt`)
SELECT up.`PolisId`,@tgl,'R',b.`recurring_seq`,1,'IDR',b.`TotalAmount`,b.`due_dt_pre`,DATE_FORMAT(b.`due_dt_pre`,'%b%y'),@bankid,
COALESCE(NULLIF(up.`AccNo`,''),NULLIF(b.`AccNo`,''),pc.`cc_no`),COALESCE(NULLIF(up.`AccName`,''),NULLIF(b.`AccName`,''),pc.`cc_name`),
2,'APPROVED',@receiptID,@receiptOtherID,@tgl
FROM " + DataHeader.stageTable + @" up
INNER JOIN `billing` b ON b.`BillingID`=@BillID
LEFT JOIN `policy_cc` pc ON pc.`PolicyId`=b.`policy_id`
WHERE up.`id`=@Id;
SELECT LAST_INSERT_ID();";
                    cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                    cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                    cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = DataProses.receiptID });
                    cmd.Parameters.Add(new MySqlParameter("@receiptOtherID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                    cmd.Parameters.Add(new MySqlParameter("@BillID", MySqlDbType.VarChar) { Value = billingID });
                    cmd.Parameters.Add(new MySqlParameter("@Id", MySqlDbType.Int32) { Value = DataProses.id });
                    cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                    DataProses.TransID = cmd.ExecuteScalar().ToString();
                }// End CC
                else if (DataHeader.source.Trim().ToUpper() == "AC")
                {
                    // Insert AC Transaction
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"
INSERT INTO `prod_life21`.policy_ac_transaction(`policy_id`,`transaction_dt`,`transaction_type`,`recurring_seq`,`count_times`,`currency`,`total_amount`,`due_date_pre`,`due_date_pre_period`,
`bank_id`,`acc_no`,`acc_name`,`status_id`,`receipt_id`,`created_dt`,`update_dt`)

SELECT up.`PolisId`,@tgl,'R',b.`recurring_seq`,1,'IDR',b.`TotalAmount`,b.`due_dt_pre`,DATE_FORMAT(b.`due_dt_pre`,'%b%y'),@bankid,COALESCE(NULLIF(up.`AccNo`,''),NULLIF(b.`AccNo`,''),pc.`acc_no`),COALESCE(NULLIF(up.`AccName`,''),NULLIF(b.`AccName`,''),pc.`acc_name`),2,@receiptID,@tgl,@tgl
FROM " + DataHeader.stageTable + @" up , `billing` b
LEFT JOIN `policy_ac` pc ON pc.`PolicyId`=b.`policy_id`
WHERE up.`id`=@Id AND b.`BillingID`=@BillID;
SELECT LAST_INSERT_ID();";
                    cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                    cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                    cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = DataProses.receiptID });
                    cmd.Parameters.Add(new MySqlParameter("@BillID", MySqlDbType.VarChar) { Value = billingID });
                    cmd.Parameters.Add(new MySqlParameter("@Id", MySqlDbType.Int32) { Value = DataProses.id });
                    cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                    DataProses.TransID = cmd.ExecuteScalar().ToString();
                }

                // Update Billing JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `billing` b
                                    LEFT JOIN `policy_cc` pc ON pc.`PolicyId`=b.`policy_id`
                                        SET b.`IsDownload`=0,
                                        b.`IsClosed`=1,
                                        b.`BillingDate`=COALESCE(b.`BillingDate`,@tgl),
                                        b.`status_billing`='P',
                                        b.`PaymentSource`=@PaymentSource,
                                        b.`paid_date`=@paid_date,
                                        b.`BankIdPaid`=@bankid,
                                        b.`PaidAmount`=@PaidAmount,
                                        b.`Life21TranID`=@TransactionID,
                                        b.`ReceiptID`=@receiptID,
                                        b.`ReceiptOtherID`=@ReceiptOtherID,
                                        b.`policy_note_receipt`=@policy_note_receipt,
                                        b.`policy_note_receiptOther`=@policy_note_receiptOther,
                                        b.`PaymentTransactionID`=@uid,
                                        b.`ACCname`=COALESCE(NULLIF(@ACCname,''),NULLIF(`ACCname`,''),pc.`cc_name`),
                                        b.`ACCno`=COALESCE(NULLIF(@ACCno,''),NULLIF(`ACCno`,''),pc.`cc_no`),
                                        #b.`cancel_date`=null,
                                        b.`LastUploadDate`=@tgl,
                                        b.`UserUpload`='system'
                                    WHERE b.`BillingID`=@idBill;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@TransactionID", MySqlDbType.Int32) { Value = DataProses.TransID });
                cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = DataProses.receiptID });
                cmd.Parameters.Add(new MySqlParameter("@ReceiptOtherID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@policy_note_receipt", MySqlDbType.Int32) { Value = DataProses.PolisNoteReceiptID });
                cmd.Parameters.Add(new MySqlParameter("@policy_note_receiptOther", MySqlDbType.Int32) { Value = DataProses.PolisNoteReceiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
                cmd.Parameters.Add(new MySqlParameter("@paid_date", MySqlDbType.DateTime) { Value = (DataHeader.source.Trim().ToUpper() == "CC" ? null : DataProses.paid_date) });
                cmd.Parameters.Add(new MySqlParameter("@PaymentSource", MySqlDbType.VarChar) { Value = DataHeader.source });
                cmd.Parameters.Add(new MySqlParameter("@ACCname", MySqlDbType.VarChar) { Value = DataProses.AccName });
                cmd.Parameters.Add(new MySqlParameter("@ACCno", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@idBill", MySqlDbType.Int32) { Value = billingID });
                cmd.ExecuteNonQuery();

                // Update Polis Last Transaction JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"INSERT INTO `policy_last_trans`(`policy_Id`,`BillingID`,`BillingDate`,`recurring_seq`,`due_dt_pre`,`source`,`receipt_id`,`receipt_date`,`bank_id`,`UserCrt`)
                            SELECT bx.policy_id, bx.`BillingID`,bx.`BillingDate`,bx.`recurring_seq`,bx.`due_dt_pre`,bx.`PaymentSource`,bx.`ReceiptID`,@tgl,bx.`BankIdDownload`,@usercrt
                            FROM `billing` AS bx
                            LEFT JOIN `policy_last_trans` AS pt ON bx.policy_id=pt.policy_Id
                            WHERE bx.BillingID=@idBill
                            ON DUPLICATE KEY UPDATE `BillingID`=bx.`BillingID`,
	                            `BillingDate`=bx.`BillingDate`,
	                            `recurring_seq`=bx.`recurring_seq`,
	                            `due_dt_pre`=bx.`due_dt_pre`,
	                            `source`=bx.`PaymentSource`,
	                            `receipt_id`=bx.`ReceiptID`,
	                            `receipt_date`=@tgl,
	                            `bank_id`=bx.`BankIdDownload`,
	                            `UserCrt`='system';";
                cmd.Parameters.Add(new MySqlParameter("@idBill", MySqlDbType.VarChar) { Value = billingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.ExecuteNonQuery();

                // Kasi Flag di data upload 
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE " + DataHeader.stageTable + @" up SET up.`IsExec`=1 WHERE up.`id`=@id ";
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = DataProses.id });
                cmd.ExecuteNonQuery();

                // Insert EmailQuee
                var emailThanks = new EmailThanksRecuring(Convert.ToInt32(billingID), DataProses.Amount, DataHeader.tglSkrg);
                emailThanks.InsertEmailQuee();
                tr.Commit();
            }
            catch (Exception ex)
            {
                if (tr != null) tr.Rollback();
                var LogError = new ErrorLog(DataHeader.trancode, DataProses.PolisNo, DataProses.IsSukses, "RecurringApprove : " + ex.Message);
            }
            finally
            {
                if (tr != null) tr.Dispose();
                con.CloseAsync();
            }
        }

        public static void BillOtherApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            string billingID = "", BilType = "", polis_id = "";
            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();

            try
            {
                GetBillingOtherUnpaid(ref billingID, ref BilType, ref polis_id, DataProses.BillingID);
                if (billingID == "") throw new Exception("Billing Other sudah Paid");
                DataProses.PolisId = polis_id;

                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                // Insert Polis Note Receipt Other
                var pesan = "RECEIPT INPUT ";
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"insert into `prod_life21`.policy_note(policy_id, date_tran, message, staff_id) 
                                    SELECT @PolisId, @tgl,@pesan,1000;
                                    SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@PolisId", MySqlDbType.Int32) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                if (BilType == "A2")
                    cmd.Parameters.Add(new MySqlParameter("@pesan", MySqlDbType.VarChar) { Value = string.Concat(pesan, "Endorsemen Cetak Polis Fisik").ToUpper() });
                else if (BilType == "A3")
                    cmd.Parameters.Add(new MySqlParameter("@pesan", MySqlDbType.VarChar) { Value = string.Concat(pesan, "Cetak KARTU").ToUpper() });
                else if (BilType == "A1")
                    cmd.Parameters.Add(new MySqlParameter("@pesan", MySqlDbType.VarChar) { Value = string.Concat(pesan, "cashlessfee").ToUpper() });
                DataProses.PolisNoteReceiptOtherID = cmd.ExecuteScalar().ToString();

                //Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
        INSERT `transaction_bank`(`File_Backup`,`TranCode`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
        values (@FileBackup,@TranCode,@PolicyId,@BillingID,@BillAmount,@ApprovalCode,@Description,@accNo,@accName);
        SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@PolicyId", MySqlDbType.VarChar) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.VarChar) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.VarChar) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalar().ToString();

                //Insert Receipt Other
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
        INSERT INTO `prod_life21`.`receipt_other`(`receipt_date`,`policy_id`,`receipt_type_id`,`receipt_amount`,`receipt_source`,`receipt_payment_date`,`receipt_seq`,`bank_acc_id`,`acquirer_bank_id`)
        SELECT @tgl,b.`policy_id`,1,b.`TotalAmount`,@source,@tgl,0,@bankAccId,@bankid
        FROM " + DataHeader.stageTable + @" up
        INNER JOIN `billing_others` b ON b.`BillingID`=up.`BillingID`
        WHERE up.`id`=@id;
        SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.VarChar) { Value = DataProses.id });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@source", MySqlDbType.VarChar) { Value = DataHeader.source });
                cmd.Parameters.Add(new MySqlParameter("@bankAccId", MySqlDbType.Int32) { Value = DataHeader.bankid_receipt });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                DataProses.receiptOtherID = cmd.ExecuteScalar().ToString();

                //Update Life21 CC Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21`.`policy_cc_transaction` pc
                                        INNER JOIN `billing_others` bo ON bo.`Life21TranID`=pc.`policy_cc_tran_id`
                                            SET pc.status_id=2,
        	                                pc.result_status=@rstStatus,
        	                                pc.Remark='APPROVED',
        	                                pc.receipt_other_id=@receiptID,
        	                                pc.update_dt=@tgl
                                            WHERE bo.`BillingID`=@id;";
                cmd.Parameters.Add(new MySqlParameter("@rstStatus", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                //Update Billing Other JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `billing_others` SET `IsDownload`=0,
        			                                `IsClosed`=1,
        			                                `status_billing`='P',
                                                    `PaymentSource`='CC',
        			                                `LastUploadDate`=@tgl,
                                                    `paid_date`=DATE(@tgl),
                                                    BankIdPaid=@bankid,
                                                    policy_note_receiptOther=@note_receiptOther,
                                                    `PaidAmount`=@PaidAmount,
        			                                `ReceiptOtherID`=@receiptOtherID,
        			                                `PaymentTransactionID`=@uid,
                                                    UserUpload='system'
        		                                WHERE `BillingID`=@idBill;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@receiptOtherID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@note_receiptOther", MySqlDbType.Int32) { Value = DataProses.PolisNoteReceiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.VarChar) { Value = DataProses.TransHistory });
                cmd.Parameters.Add(new MySqlParameter("@idBill", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                // Kasi Flag di data upload 
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE " + DataHeader.stageTable + @" up SET up.`IsExec`=1 WHERE up.`id`=@id ";
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = DataProses.id });
                cmd.ExecuteNonQuery();

                var emailEndorsThanks = new EmailThanksEndorsemen(DataProses.BillingID, DataProses.Amount, DataHeader.tglSkrg);
                emailEndorsThanks.InsertEmailQuee();

                tr.Commit();
            }
            catch (Exception ex)
            {
                if (tr != null) tr.Rollback();
                var LogError = new ErrorLog(DataHeader.trancode, DataProses.PolisNo, DataProses.IsSukses, "BillOtherApprove : " + ex.Message);
            }
            finally
            {
                if (tr != null) tr.Dispose();
                con.CloseAsync();
            }
        }

        public static void QuoteApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            if ((DataProses.BillingID == null) || (DataProses.BillingID == "")) return;
            string billingID = "";

            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();
            try
            {
                GetQuoteUnpaid(ref billingID, DataProses.BillingID);
                if (billingID == "") throw new Exception("Billing Quote sudah Paid");

                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                ////Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
        INSERT `transaction_bank`(`File_Backup`,`TranCode`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
        values (@FileBackup,@TranCode,@BillingID,@BillAmount,@ApprovalCode,@Description,@accNo,@accName);
        SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.VarChar) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.VarChar) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalar().ToString();

                ////Update status Quote jadi Paid
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21p`.`quote` q
                                                SET q.`quote_status`='P',
                                                quote_submitted_dt=@tgl
                                                WHERE q.`quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.ExecuteNonQueryAsync().Wait();

                ////Update Prospect Billing jadi approve
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21p`.`prospect_billing`
                                                SET prospect_convert_flag=2,prospect_appr_code='UP4Y1',
                                                updated_dt=@tgl,
                                                acquirer_bank_id=@bankid
                                                WHERE `quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                ////Update quote_edc jadi approve
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21p`.`quote_edc`
                                                SET status_id=1,
                                                reason='',
                                                appr_code='UP4Y1'
                                                WHERE `quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                ////Update Quote JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `quote_billing` SET `IsDownload`=0,
        			                                    `IsClosed`=1,
        			                                    `status`='P',
                                                        `PaymentSource`='CC',
                                                        `PaidAmount`=@PaidAmount,
                                                        BankIdPaid=@bankid,
        			                                    `LastUploadDate`=@tgl,
                                                        `cancel_date`=null,
                                                        `paid_dt`=DATE(@tgl),
        			                                    `PaymentTransactionID`=@uid,
                                                        UserUpload='system'
        		                                    WHERE `quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
                cmd.ExecuteNonQuery();

                // Kasi Flag di data upload 
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE " + DataHeader.stageTable + @" up SET up.`IsExec`=1 WHERE up.`id`=@id ";
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = DataProses.id });
                cmd.ExecuteNonQuery();

                ////Insert EmailQuee
                var emailQuoteThanks = new EmailThanksQuote(Convert.ToInt32(DataProses.BillingID), DataProses.Amount, DataHeader.tglSkrg);
                emailQuoteThanks.InsertEmailQuee();

                tr.Commit();
            }
            catch (Exception ex)
            {
                if (tr != null) tr.Rollback();
                var LogError = new ErrorLog(DataHeader.trancode, DataProses.PolisNo, DataProses.IsSukses, "QuoteApprove : " + ex.Message);
            }
            finally
            {
                if (tr != null) tr.Dispose();
                con.CloseAsync();
            }
        }

        public static void SubmitRejectTransaction(FileResultModel DataHeader)
        {
            Console.WriteLine();
            Console.WriteLine("Reject Transaction Begin ....");

            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand
            {
                Connection = con,
                CommandType = CommandType.Text,
                CommandText = @"
SET @tgl:=NOW();
SET @tglSaja:=DATE(@tgl);

SET @prev_value := 0;SET @rank_count := 0;

DROP TEMPORARY TABLE IF EXISTS billx;
CREATE TEMPORARY TABLE billx AS	
SELECT CASE
	    WHEN @prev_value = b.policy_id THEN @rank_count := @rank_count + 1
	    WHEN @prev_value := b.policy_id THEN @rank_count:=1
	END AS seqno,b.policy_id,b.BillingID
FROM (
	SELECT DISTINCT bx.policy_id,bx.BillingID
	FROM `billing` bx
	INNER JOIN " + DataHeader.stageTable + @" su ON su.`PolisId`=bx.policy_id
	WHERE bx.status_billing IN ('A','C') AND su.`IsSukses`=0 AND su.IsExec=0
)b;

SET @prev_value := 0;SET @rank_count := 0;

DROP TEMPORARY TABLE IF EXISTS billu;
CREATE TEMPORARY TABLE billu AS	
SELECT CASE
	    WHEN @prev_value = su.PolisId THEN @rank_count := @rank_count + 1
	    WHEN @prev_value := su.PolisId THEN @rank_count:=1
	END AS seqno,su.id, su.PolisId
FROM " + DataHeader.stageTable + @" su
WHERE su.`IsSukses`=0 AND su.`BillCode`='B' AND su.IsExec=0
ORDER BY su.`PolisId`,su.`amount`;

# Update data upload Recurring dari hasil mapping
UPDATE " + DataHeader.stageTable + @" up
INNER JOIN billu bu ON bu.id=up.`id`
INNER JOIN billx bx ON bx.policy_id=bu.PolisId AND bx.seqno=bu.seqno
" + ((DataHeader.Id == 2) ? "LEFT JOIN `reason_maping_group` rp ON rp.`RejectCode`=up.`ApprovalCode`" : "LEFT JOIN `reason_maping_group` rp ON rp.`ReajectReason`=up.`Deskripsi`") + @"
	SET up.`BillingID`=bx.BillingID,
	up.`Deskripsi`=COALESCE(rp.`ReajectReason`,up.`Deskripsi`,up.`ApprovalCode`),
	up.`RejectGroupID`=rp.`GroupRejectMappingID`
WHERE up.IsExec=0 AND up.IsSukses=0;

# Update data upload BillingOther dari hasil mapping
UPDATE " + DataHeader.stageTable + @" up
INNER JOIN `billing_others` bo ON bo.`BillingID`=up.`BillingID`
" + ((DataHeader.Id == 2) ? "LEFT JOIN `reason_maping_group` rp ON rp.`RejectCode`=up.`ApprovalCode`" : "LEFT JOIN `reason_maping_group` rp ON rp.`ReajectReason`=up.`Deskripsi`") + @"
	SET up.`BillingID`=up.`BillingID`,
	up.`Deskripsi`=COALESCE(rp.`ReajectReason`,up.`Deskripsi`,up.`ApprovalCode`),
	up.`RejectGroupID`=rp.`GroupRejectMappingID`
WHERE up.IsExec=0 AND up.IsSukses=0;

# Update data upload Quote dari hasil mapping
UPDATE " + DataHeader.stageTable + @" up
INNER JOIN `quote_billing` q ON q.`quote_id`=up.`BillingID`
" + ((DataHeader.Id == 2) ? "LEFT JOIN `reason_maping_group` rp ON rp.`RejectCode`=up.`ApprovalCode`" : "LEFT JOIN `reason_maping_group` rp ON rp.`ReajectReason`=up.`Deskripsi`") + @"
	SET up.`BillingID`=up.`BillingID`,
	up.`Deskripsi`=COALESCE(rp.`ReajectReason`,up.`Deskripsi`,up.`ApprovalCode`),
	up.`RejectGroupID`=rp.`GroupRejectMappingID`
WHERE up.IsExec=0 AND up.IsSukses=0;

SELECT `AUTO_INCREMENT` INTO @tbid
FROM  INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = @db
AND   TABLE_NAME   = 'transaction_bank';

INSERT INTO `transaction_bank`(`File_Backup`,`TranCode`,`IsSuccess`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`GroupRejectMappingID`,`accNo`,`accName`,`DateInsert`)
SELECT u.`FileName`,fp.`trancode`,0,u.`PolisId`,u.`BillingID`,u.`Amount`,u.`ApprovalCode`,
u.`Deskripsi`,u.`RejectGroupID`,u.`AccNo`,u.`AccName`,@tgl
FROM `FileNextProcess` fp
INNER JOIN " + DataHeader.stageTable + @" u ON u.`FileName`=fp.`FileName`
WHERE fp.`id`=@idx AND fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL
AND u.`IsExec`=0 AND u.`IsSukses`=0 AND u.`BillingID` IS NOT NULL;

UPDATE `billing` b
INNER JOIN transaction_bank tb ON tb.`BillingID`=b.BillingID
	SET b.`IsDownload`=0,
	b.`LastUploadDate`=@tgl,
	b.`PaymentTransactionID`=tb.`id`,
 	b.`BankIdDownload`=COALESCE(b.`BankIdDownload`,@BankIDDwd),
	b.`Source_download`=COALESCE(b.`Source_download`,'" + DataHeader.source + @"'),
	b.`BillingDate`=COALESCE(b.`BillingDate`,@tglSaja)
WHERE b.`status_billing` IN ('A','C')
	AND tb.`id` >= @tbid
	AND tb.`DateInsert` >= @tgl;
	
UPDATE `billing_others` q
INNER JOIN transaction_bank tb ON tb.`BillingID`=q.`BillingID`
	SET q.`IsDownload`=0,q.`LastUploadDate`=@tgl,q.`PaymentTransactionID`=tb.`id`,q.`Source_download`='CC'
WHERE q.`status_billing` IN ('A','C')
	AND tb.`id` >= @tbid
	AND tb.`DateInsert` >= @tgl;		
		
UPDATE `quote_billing` q
INNER JOIN transaction_bank tb ON tb.`BillingID`=q.`quote_id`
	SET q.`IsDownload`=0,q.`LastUploadDate`=@tgl,q.`PaymentTransactionID`=tb.`id`,q.`Source_download`='CC'
WHERE q.`status` IN ('A','C')
	AND tb.`id` >= @tbid
	AND tb.`DateInsert` >= @tgl;"
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = DataHeader.Id });
            cmd.Parameters.Add(new MySqlParameter("@db", MySqlDbType.VarChar) { Value = cmd.Connection.Database });
            cmd.Parameters.Add(new MySqlParameter("@BankIDDwd", MySqlDbType.Int32) { Value = DataHeader.bankid });
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex) { throw new Exception("SubmitRejectTransaction() : " + ex.Message); }
            finally { con.CloseAsync(); }
        }

        public static void ExceuteDataUpload()
        {
            Boolean stop = true;
            List<DataSubmitModel> DataProses;
            while (stop)
            {
                DataProses = new List<DataSubmitModel>();
                try
                {
                    var Fileproses = GenFile();
                    foreach (FileResultModel item in Fileproses)
                    {
                        //MapingData(item.Id, item.stageTable);
                        Thread.Sleep(5000);
                        DataProses = new List<DataSubmitModel>();
                        if (item.Id != 2) DataProses = PoolDataProsesApprove(item.Id, item.stageTable);

                        if (DataProses.Count > 0) SubmitApproveTransaction(item.stageTable, DataProses, item);

                        //Proses yang reject
                        if (item.Id > 1) SubmitRejectTransaction(item);

                        RemoveFileUploadResult(item);
                        RemoveFileBilling(item);
                    }
                    stop = false;
                }
                catch (Exception ex) { Console.WriteLine(ex.Message); }
            }
        }

        public static void ExceuteDataUpload(int id)
        {
            List<DataSubmitModel> DataProses;
            DataProses = new List<DataSubmitModel>();
            try
            {
                var Fileproses = GenFile(id);
                //MapingData(Fileproses.Id, Fileproses.stageTable);
                if (Fileproses == null) return;

                DataProses = new List<DataSubmitModel>();
                if (Fileproses.Id != 2) DataProses = PoolDataProsesApprove(Fileproses.Id, Fileproses.stageTable);
                if (DataProses.Count > 0) SubmitApproveTransaction(Fileproses.stageTable, DataProses, Fileproses);

                //Proses yang reject
                if (id != 1) SubmitRejectTransaction(Fileproses);

                RemoveFileUploadResult(Fileproses);
                RemoveFileBilling(Fileproses);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Thread.Sleep(5000);
            }
        }

        public static void GetBillingUnpaid(ref string BillingID, ref int freq_pay, ref decimal cashlessFee, string PolisID)
        {
            GetLastBillingUnpaid(ref BillingID, ref freq_pay, ref cashlessFee, PolisID);

            //// Create Billing jika billing tidak ada pada saat mapping (karena Approve)
            if (BillingID == "")
            {
                MySqlConnection con = new MySqlConnection(constring);
                MySqlCommand cmd = new MySqlCommand();
                cmd = new MySqlCommand("CreateNewBillingRecurring", con)
                {
                    CommandType = CommandType.StoredProcedure
                };
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new MySqlParameter("@polisId", MySqlDbType.VarChar) { Value = PolisID });
                try
                {
                    cmd.Connection.Open();
                    BillingID = cmd.ExecuteScalar().ToString(); // ini bisa diabaikan, karena akan dicek lagi
                }
                catch (Exception ex) { throw new Exception("GetBillingUnpaid() : " + ex.Message); }
                finally { con.CloseAsync(); }

                GetLastBillingUnpaid(ref BillingID, ref freq_pay, ref cashlessFee, PolisID); // pengecekan kedua setelah create billing
            }

            if (BillingID == "") throw new Exception("Billing Konsong setelah CreateBilling, untuk PolisID '" + PolisID + "'");
        }

        public static void GetLastBillingUnpaid(ref string BillingID, ref int freq_pay, ref decimal cashlessFee, string PolisID)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand(@"SELECT b.`BillingID`,b.`policy_id`,b.`cashless_fee_amount`,b.`freq_payment`
                            FROM `policy_billing` pb
                            INNER JOIN `billing` b ON pb.`policy_Id`=b.`policy_id`
                            WHERE pb.`policy_id`=@PolisID AND b.`status_billing` IN ('A','C')
                            ORDER BY b.`recurring_seq` ASC
                            LIMIT 1;")
            {
                CommandType = CommandType.Text,
                Connection = con
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@PolisID", MySqlDbType.Int32) { Value = PolisID });

            try
            {
                cmd.Connection.Open();
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        BillingID = rd["BillingID"].ToString();
                        decimal.TryParse(rd["cashless_fee_amount"].ToString(), out cashlessFee);
                        int.TryParse(rd["freq_payment"].ToString(), out freq_pay);
                    }

                }
            }
            catch (Exception ex) { throw new Exception("GetLastBillingUnpaid() : " + ex.Message); }
            finally { con.Close(); }

        }

        public static void GetBillingUnpaidForReject(ref string BillingID, string PolisID, string tableName)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand(@"SELECT b.`BillingID`,b.`policy_id`
                            FROM `policy_billing` pb
                            INNER JOIN `billing` b ON pb.`policy_Id`=b.`policy_id`
                            LEFT JOIN " + tableName + @" up ON up.`BillingID`=b.`BillingID`
                            WHERE pb.`policy_id`=@PolisID AND b.`status_billing` IN ('A','C') AND up.`BillingID` IS NULL
                            ORDER BY b.`recurring_seq` ASC
                            LIMIT 1;")
            {
                CommandType = CommandType.Text,
                Connection = con
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@PolisID", MySqlDbType.Int32) { Value = PolisID });

            try
            {
                cmd.Connection.Open();
                using (var rd = cmd.ExecuteReader()) while (rd.Read()) BillingID = rd["BillingID"].ToString();
            }
            catch (Exception ex) { throw new Exception("GetBillingUnpaidForReject() : " + ex.Message); }
            finally { con.CloseAsync(); }
        }

        public static void GetBillingOtherUnpaid(ref string BillingID, ref string BillingType, ref string Polis_id, string BillOthersID)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand(@"SELECT b.`BillingID`,b.`BillingType`,b.`policy_id` FROM `billing_others` b
                                                WHERE b.`BillingID`=@BillingID AND b.`status_billing` IN ('A','C');", con);
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = BillOthersID });

            try
            {
                cmd.Connection.Open();
                var rd = cmd.ExecuteReader();
                while (rd.Read())
                {
                    BillingID = rd["BillingID"].ToString();
                    BillingType = rd["BillingType"].ToString();
                    Polis_id = rd["policy_id"].ToString();
                }
                //BillingID = (cmd.ExecuteScalar() == null) ? "" : cmd.ExecuteScalar().ToString();
            }
            catch (Exception ex) { throw new Exception("GetBillingOtherUnpaid() : " + ex.Message); }
            finally { con.CloseAsync(); }
        }

        public static void GetQuoteUnpaid(ref string BillingID, string QuoteID)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand(@"SELECT q.`quote_id` FROM `quote_billing` q 
                                                WHERE q.`quote_id`=@BillingID AND q.`status` IN ('A','C');", con);
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.Int32) { Value = QuoteID });

            try
            {
                cmd.Connection.Open();
                BillingID = cmd.ExecuteScalar().ToString();
            }
            catch (Exception ex) { throw new Exception("GetQuoteUnpaid() : " + ex.Message); }
            finally { con.CloseAsync(); }
        }

        public static void UpdateBilling_VA_Paid(int idx, string tableName)
        {
            //cek data upload berdasarkan polis_no dan tgl bayar ==> tgl bayar unik pertransaksi
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand()
            {
                CommandText = @"UPDATE " + tableName + @" up
                            INNER JOIN `billing` b ON b.`policy_id`=up.`PolisId` AND up.`TranDate`=b.`paid_date` AND b.`status_billing`='P'
                            SET up.`IsExec`=1; ",
                CommandType = CommandType.Text,
                Connection = con,
            };
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = idx });

            try
            {
                cmd.Connection.Open();
                cmd.ExecuteNonQuery();

                cmd.CommandText = "DELETE va FROM " + tableName + @" va WHERE va.`IsExec`";
                cmd.Parameters.Clear();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex) { throw new Exception("UpdateBilling_VA_Paid() : " + ex.Message); }
            finally { con.CloseAsync(); }
        }
    }
}
