using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
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
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];
        static string FileResult = ConfigurationManager.AppSettings["DirResult"];
        static string FileBackup = ConfigurationManager.AppSettings["BackupResult"];

        static string FileBilling = ConfigurationManager.AppSettings["FileBilling"];
        static string BillingBackup = ConfigurationManager.AppSettings["BillingBackup"];
        static DateTime TglNow = DateTime.Now;
        static DateTime Tgl = DateTime.Now.Date;

        static void Main(string[] args)
        {
            //args = new string[] { "exec" };
            //args = new string[] { "upload", "6" };
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
                    if (!File.OpenRead(FileResult + FileUpload.FileName).CanRead)
                    {
                        Console.WriteLine("File tidak bisa di proses");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }

                    var IsData = CekFileInsert(idx, FileUpload.stageTable);
                    // Jika data sudah pernah diinsert atas file tersebut -> exit
                    if (CekFileInsert(idx, FileUpload.stageTable))
                    {
                        Console.WriteLine("Data Sudah Pernah insert . . . ");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(5000);
                        return;
                    }
                    else if (!(idx == 1 || idx == 2)) KosongkanTabel(FileUpload.stageTable);

                    List<DataUploadModel> DataUpload = new List<DataUploadModel>();
                    if (idx == 1)
                    {
                        // kosongkan table jika file bca yang satunya belum diupload (BCA Reject)
                        if (!CekFileInsert(2, FileUpload.stageTable)) KosongkanTabel(FileUpload.stageTable);
                        DataUpload = BacaFileBCA(FileUpload.FileName);
                    }
                    else if (idx == 2)
                    {
                        if (!CekFileInsert(1, FileUpload.stageTable)) KosongkanTabel(FileUpload.stageTable);
                        DataUpload = BacaFileBCA(FileUpload.FileName);
                    }
                    else if (idx == 3) DataUpload = BacaFileMandiri(FileUpload.FileName);
                    else if (idx == 4 || idx == 5) DataUpload = BacaFileMega(FileUpload.FileName);
                    else if (idx == 6) DataUpload = BacaFileBNI(FileUpload.FileName);

                    InsertTableStaging(DataUpload, FileUpload.stageTable, FileUpload.FileName);
                    MapingData(idx, FileUpload.stageTable);
                }
            }
            else if (args[0] == "exec")
            {
                if (args.Count() == 1) ExceuteDataUpload();
                if (args.Count() > 1)
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

        public static FileResultModel GetUploadFile(int id)
        {
            FileResultModel Fileproses = new FileResultModel();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM `FileNextProcess`
                                    WHERE `FileName` IS NOT NULL AND `tglProses` IS NOT NULL
                                    AND id=@idx;", con)
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
                            FileBilling = rd["FileBilling"].ToString(),
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
            cmd = new MySqlCommand(@"SELECT * FROM `FileNextProcess`
                                    WHERE `FileName` IS NOT NULL AND `tglProses` IS NOT NULL
                                    AND `tglProses` = CURDATE();", con)
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
                            FileBilling = rd["FileBilling"].ToString(),
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
            FileResultModel Fileproses = new FileResultModel();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM `FileNextProcess`
                                    WHERE `FileName` IS NOT NULL AND `tglProses` IS NOT NULL
                                    AND `tglProses` = CURDATE() AND id=@idx;", con)
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
                            FileBilling = rd["FileBilling"].ToString(),
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
                            PolisId = rd["PolisId"].ToString(),
                            //BillingID = (rd["BillingID"].ToString() == string.Empty) ? null : rd["BillingID"].ToString(),
                            BillCode = rd["BillCode"].ToString(),
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

            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsExec`=0 AND u.`IsSukses`=1 AND u.BillCode<>'B';", con)
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

        public static List<DataSubmitModel> PoolDataProsesReject(int id, string tableName)
        {
            //IsSukses 0=Reject, 1=Approve
            Console.Write("Pooling data Reject ... " + tableName + " ... ");
            List<DataSubmitModel> DataProses = new List<DataSubmitModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT u.* 
                                    FROM `FileNextProcess` fp
                                    INNER JOIN " + tableName + @" u ON u.`FileName`=fp.`FileName`
                                    WHERE fp.`id`=@idx AND fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL
                                    AND fp.`tglProses`=CURDATE() AND u.`IsExec`=0 AND u.`IsSukses`=0 AND u.BillCode='B';")
            {
                CommandType = CommandType.Text,
                Connection = con
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
                        });
                    }
                }
            }
            catch (Exception ex) { throw new Exception("PoolDataProsesReject(B) : " + ex.Message); }
            finally { con.Close(); }

            cmd = new MySqlCommand(@"SELECT u.* 
                                    FROM `FileNextProcess` fp
                                    INNER JOIN " + tableName + @" u ON u.`FileName`=fp.`FileName`
                                    WHERE fp.`id`=@idx AND fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL
                                    AND fp.`tglProses`=CURDATE() AND u.`IsExec`=0 AND u.`IsSukses`=0 AND u.BillCode<>'B';")
            {
                CommandType = CommandType.Text,
                Connection = con
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
                throw new Exception("PoolDataProsesReject(X) : " + ex.Message);
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

        public static void RemoveFile(FileResultModel Fileproses)
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
                con.Open();
                FileInfo Filex = new FileInfo(FileResult + Fileproses.FileName);
                if (Filex.Exists)
                {
                    Filex.MoveTo(FileBackup + Fileproses.FileName);
                    cmd.ExecuteNonQuery();
                }
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
                cmd.Parameters.Add(new MySqlParameter("@idd", MySqlDbType.Int32) { Value = Fileproses.id_billing_download });

                cmd.Connection.Open();
                var data = cmd.ExecuteScalar();
                cmd.Connection.Close();

                if (!Decimal.TryParse(data.ToString(), out decimal itemData)) return;
                if (itemData > 0) return;

                cmd = new MySqlCommand(@"UPDATE `FileNextProcess` SET `FileBilling`=NULL WHERE `id`=@id;")
                {
                    CommandType = CommandType.Text,
                    Connection = con
                };
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = Fileproses.Id });

                FileInfo Filex = new FileInfo(FileBilling.Trim() + Fileproses.FileBilling.Trim());
                if (Filex.Exists)
                {
                    Filex.MoveTo(BillingBackup.Trim() + Fileproses.FileBilling.Trim() + Regex.Replace(Guid.NewGuid().ToString(), "[^0-9a-zA-Z]", "").Substring(0, 8));
                    cmd.Connection.Open();
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex) { throw new Exception("RemoveFileBilling() : " + ex.Message); }
            finally
            {
                if (cmd.Connection.State == ConnectionState.Open) cmd.Connection.CloseAsync();
            }
        }

        public static void InsertTableStaging(List<DataUploadModel> DataUpload, string tableName, string FileName)
        {
            String sqlStart = @"INSERT INTO " + tableName + "(PolisNo,Amount,ApprovalCode,Deskripsi,AccNo,AccName,IsSukses,FileName) values ";
            string sql = "";
            int i = 0, j = 1;
            foreach (DataUploadModel item in DataUpload)
            {
                if (item == null) continue;
                i++;
                sql = sql + string.Format(@"('{0}',{1},'{2}',NULLIF('{3}',''),'{4}','{5}',{6},'{7}'),",
                    item.PolisNo, item.Amount, item.ApprovalCode, (item.Deskripsi == null) ? null : item.Deskripsi.Replace("'", "\\'"),
                    item.AccNo, (item.AccName == null) ? null : item.AccName.Replace("'", "\\'"), item.IsSukses, FileName);
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

        public static List<DataUploadModel> BacaFileBCA(string Fileproses)
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

        public static List<DataUploadModel> BacaFileMandiri(string Fileproses)
        {
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
                        AccName = ws.Cells[row, 2].Value.ToString().Trim(),
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
                        AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        PolisNo = ws.Cells[row, 4].Value.ToString().Trim(),
                        ApprovalCode = (ws.Cells[row, 5].Value ?? "").ToString().Trim(),
                        Deskripsi = (ws.Cells[row, 6].Value ?? "").ToString().Trim(),
                        AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = false
                    });
                }
                fs.Close();
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileMega(string Fileproses)
        {
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
                        //AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 5].Value.ToString().Trim(),
                        PolisNo = temp.Split('-').Last().Trim(),
                        //AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
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
                        //AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 5].Value.ToString().Trim(),
                        PolisNo = temp.Split('-').Last().Trim(),
                        //AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = false
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileBNI(string Fileproses)
        {
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
                        AccName = ws.Cells[row, 5].Value.ToString().Trim(),
                        Deskripsi = ws.Cells[row, 10].Value.ToString().Trim(),
                        IsSukses = (ws.Cells[row, 9].Value.ToString().Trim() == "") ? false : true
                    });
                }
            }
            return dataUpload;
        }

        public static void KosongkanTabel(string TableName)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"DELETE FROM " + TableName + ";", con);
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

        public static void MapingData(int idx, string tableName)
        {
            Console.WriteLine("Mapping data  ...");
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;

            cmd = new MySqlCommand(@"
UPDATE " + tableName + @" up
INNER JOIN `policy_billing` pb ON pb.`policy_no`=up.`PolisNo`
	SET up.`PolisId`=pb.`policy_Id`,up.`BillCode`='B'
WHERE up.`IsExec`=0 AND LEFT(up.`PolisNo`,1) NOT IN ('A','X');

UPDATE " + tableName + @" up
INNER JOIN `billing_others` bo ON bo.`BillingID`=up.`PolisNo`
	SET up.`BillingID`=up.`PolisNo`,up.`BillCode`='A'
WHERE up.`IsExec`=0 AND LEFT(up.`PolisNo`,1) ='A';

UPDATE " + tableName + @" up
LEFT JOIN `quote_billing` q ON q.`quote_id`=SUBSTRING_INDEX(up.`PolisNo`,'X',-1)
	SET up.`BillingID`=q.`quote_id`,up.`BillCode`='Q'
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
                    else if (item.BillCode == "A") BillOtherApprove(item, DataHeader);
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
            int CashlessFeeAmount = 0;

            try
            {
                GetBillingUnpaid(ref billingID, DataProses.PolisId);
                if (billingID == "") throw new Exception("Billing Kosong....");

                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                // Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT `transaction_bank`(`File_Backup`,`TranCode`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
values (@FileBackup,@TranCode,@PolicyId,@BillingID,@BillAmount,@ApprovalCode,@Description,@accNo,@accName);
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
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
`receipt_source`, `receipt_status`, `receipt_payment_date_time`, `receipt_seq`, `bank_acc_id`, `due_date_pre`,`acquirer_bank_id`)
SELECT @tgl,up.`PolisId`,0,'RP',up.`Amount`-b.`cashless_fee_amount`,@source,'P',@tgl,b.`recurring_seq`,@bankAccId,b.`due_dt_pre`,@bankid
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
                DataProses.receiptID = cmd.ExecuteScalar().ToString();

                // Insert Receipt Other
                if (CashlessFeeAmount > 0)
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt_other`(`receipt_date`,`policy_id`,`receipt_type_id`,`receipt_amount`,`receipt_source`,`receipt_payment_date`,`receipt_seq`,`bank_acc_id`,`acquirer_bank_id`)
SELECT @tgl,b.`policy_id`,3,b.`cashless_fee_amount`,@source,@tgl,b.`recurring_seq`,@bankAccId,@bankid
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
                    DataProses.receiptOtherID = cmd.ExecuteScalar().ToString();
                }

                // Insert CC Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT INTO `prod_life21`.`policy_cc_transaction`(`policy_id`,`transaction_dt`,`transaction_type`,`recurring_seq`,
`count_times`,`currency`,`total_amount`,`due_date_pre`,`due_date_pre_period`,`acquirer_bank_id`,
`cc_no`,`cc_name`,`status_id`,`remark`,`receipt_id`,`receipt_other_id`,`created_dt`)
SELECT up.`PolisId`,@tgl,'R',b.`recurring_seq`,1,'IDR',b.`TotalAmount`,b.`due_dt_pre`,DATE_FORMAT(b.`due_dt_pre`,'%b%d'),@bankid,
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

                // Update Billing JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `billing` b
                                    LEFT JOIN `policy_cc` pc ON pc.`PolicyId`=b.`policy_id`
                                        SET b.`IsDownload`=0,
                                        b.`IsClosed`=1,
                                        b.`BillingDate`=COALESCE(b.`BillingDate`,@tgl),
                                        b.`status_billing`='P',
                                        b.`PaymentSource`='CC',
                                        b.`BankIdPaid`=@bankid,
                                        b.`PaidAmount`=@PaidAmount,
                                        b.`Life21TranID`=@TransactionID,
                                        b.`ReceiptID`=@receiptID,
                                        b.`ReceiptOtherID`=@ReceiptOtherID,
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
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
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
            string billingID = "";
            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();

            try
            {
                GetBillingOtherUnpaid(ref billingID, DataProses.BillingID);
                if (billingID == "") throw new Exception("Billing Other sudah Paid");

                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

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
                                                    `PaidAmount`=@PaidAmount,
        			                                `ReceiptOtherID`=@receiptOtherID,
        			                                `PaymentTransactionID`=@uid,
                                                    UserUpload='system'
        		                                WHERE `BillingID`=@idBill;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@receiptOtherID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
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

DELETE su FROM " + DataHeader.stageTable + @" su WHERE su.`IsSukses`=1 ;

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
	WHERE bx.status_billing IN ('A','C')
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
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.ApprovalCode
LEFT JOIN `ReasonMapingGroup` rg ON rg.`ReajectReason`=COALESCE(up.`Deskripsi`,rm.`reject_reason_bank`)
	SET up.`BillingID`=bx.BillingID,
	up.`Deskripsi`=COALESCE(rm.`reject_reason_bank`,up.`Deskripsi`,up.`ApprovalCode`),
	up.`RejectGroupID`=rg.`GroupRejectMappingID`
WHERE up.IsExec=0;

# Update data upload BillingOther dari hasil mapping
UPDATE " + DataHeader.stageTable + @" up
INNER JOIN `billing_others` bo ON bo.`BillingID`=up.`BillingID`
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.ApprovalCode
LEFT JOIN `ReasonMapingGroup` rg ON rg.`ReajectReason`=COALESCE(up.`Deskripsi`,rm.`reject_reason_bank`)
	SET up.`BillingID`=up.`BillingID`,
	up.`Deskripsi`=COALESCE(rm.`reject_reason_bank`,up.`Deskripsi`,up.`ApprovalCode`),
	up.`RejectGroupID`=rg.`GroupRejectMappingID`
WHERE up.IsExec=0;

# Update data upload Quote dari hasil mapping
UPDATE " + DataHeader.stageTable + @" up
INNER JOIN `quote_billing` q ON q.`quote_id`=up.`BillingID`
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.ApprovalCode
LEFT JOIN `ReasonMapingGroup` rg ON rg.`ReajectReason`=COALESCE(up.`Deskripsi`,rm.`reject_reason_bank`)
	SET up.`BillingID`=up.`BillingID`,
	up.`Deskripsi`=COALESCE(rm.`reject_reason_bank`,up.`Deskripsi`,up.`ApprovalCode`),
	up.`RejectGroupID`=rg.`GroupRejectMappingID`
WHERE up.IsExec=0;

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
AND u.`IsExec`=0 AND u.`IsSukses`=0 ;

UPDATE `billing` b
INNER JOIN transaction_bank tb ON tb.`BillingID`=b.BillingID
	SET b.`IsDownload`=0,
	b.`LastUploadDate`=@tgl,
	b.`PaymentTransactionID`=tb.`id`,
 	b.`BankIdDownload`=@BankIDDwd,
	b.`Source_download`='CC',
	b.`BillingDate`=COALESCE(b.`BillingDate`,@tglSaja)
WHERE b.`status_billing` IN ('A','C')
	AND tb.`id` >= @tbid
	AND tb.`DateInsert` >= @tgl;
	
UPDATE `billing_others` q
INNER JOIN transaction_bank tb ON tb.`BillingID`=q.`BillingID`
	SET q.`IsDownload`=0,q.`LastUploadDate`=@tgl,q.`PaymentTransactionID`=tb.`id`
WHERE q.`status_billing` IN ('A','C')
	AND tb.`id` >= @tbid
	AND tb.`DateInsert` >= @tgl;		
		
UPDATE `quote_billing` q
INNER JOIN transaction_bank tb ON tb.`BillingID`=q.`quote_id`
	SET q.`IsDownload`=0,q.`LastUploadDate`=@tgl,q.`PaymentTransactionID`=tb.`id`
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
                        Thread.Sleep(5000);
                        DataProses = new List<DataSubmitModel>();
                        DataProses = PoolDataProsesApprove(item.Id, item.stageTable);
                        if (DataProses.Count > 0) SubmitApproveTransaction(item.stageTable, DataProses, item);

                        //Proses yang reject
                        if (item.Id == 1) continue; // Pada Saat Proses BCA Approve, Proses reject di skip dan akan di proses pada id 2(Bca Rejct)
                        SubmitRejectTransaction(item);

                        RemoveFile(item);
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
                if (Fileproses == null) return;
                DataProses = new List<DataSubmitModel>();
                DataProses = PoolDataProsesApprove(Fileproses.Id, Fileproses.stageTable);
                SubmitApproveTransaction(Fileproses.stageTable, DataProses, Fileproses);

                //Proses yang reject
                if (id != 1)  // Pada Saat Proses BCA Approve, Proses reject di skip dan akan di proses pada id 2(Bca Rejct)
                SubmitRejectTransaction(Fileproses);

                RemoveFile(Fileproses);
                RemoveFileBilling(Fileproses);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Thread.Sleep(5000);
            }
        }

        public static void GetBillingUnpaid(ref string BillingID, string PolisID)
        {
            MySqlConnection con = new MySqlConnection(constring);

            MySqlCommand cmd = new MySqlCommand(@"SELECT b.`BillingID`,b.`policy_id`
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
                using (var rd = cmd.ExecuteReader()) while (rd.Read()) BillingID = rd["BillingID"].ToString();
            }
            catch (Exception ex) { throw new Exception("GetBillingID() : " + ex.Message); }
            finally { con.Close(); }

            //// Create Billing jika billing tidak ada pada saat mapping (karena Approve)
            if (BillingID == "")
            {
                cmd = new MySqlCommand("CreateNewBillingRecurring", con)
                {
                    CommandType = CommandType.StoredProcedure
                };
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new MySqlParameter("@polisId", MySqlDbType.VarChar) { Value = PolisID });
                try
                {
                    cmd.Connection.Open();
                    BillingID = cmd.ExecuteScalar().ToString();
                }
                catch (Exception ex) { throw new Exception("GetBillingID() : " + ex.Message); }
                finally { con.CloseAsync(); }
            }

            if (BillingID == "") throw new Exception("Billing Konsong setelah CreateBilling, untuk PolisID '" + PolisID + "'");
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

        public static void GetBillingOtherUnpaid(ref string BillingID, string BillOthersID)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand(@"SELECT b.`BillingID` FROM `billing_others` b 
                                                WHERE b.`BillingID`=@BillingID AND b.`status_billing` IN ('A','C');", con);
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = BillOthersID });

            try
            {
                cmd.Connection.Open();
                BillingID = (cmd.ExecuteScalar() == null) ? "" : cmd.ExecuteScalar().ToString();
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
    }
}
