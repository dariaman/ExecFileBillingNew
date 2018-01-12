using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class ErrorLog
    {
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];

        public string trancode { get; set; }
        public string PolisNo { get; set; }
        public Boolean IsSukses { get; set; }
        public string ErrorMessage { get; set; }

        public ErrorLog(string trnCode, string polisno,Boolean isSukses,string PesanError)
        {
            this.trancode = trnCode;
            this.PolisNo = polisno;
            this.IsSukses = isSukses;
            this.ErrorMessage = (PesanError.Length < 300) ? PesanError : PesanError.Substring(0,299);
            try
            {
                InsertLog();
            }
            catch(Exception ex)
            {
                throw new Exception(ex.Message);
                //Console.WriteLine();
                //Console.Write(" >> GAGAL LOG : " + ex.Message);
            }
        }

        private void InsertLog()
        {
            MySqlConnection con = new MySqlConnection(constring);
            con.Open();
            MySqlCommand cmd = new MySqlCommand(@"INSERT INTO `ErrorLogUpload`(`trancode`,`PolisNo`,`IsSukses`,`ErrorMessage`)
                                        SELECT @TranCode,@PolisNo,@IsSukses,@ErrorMessage", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = this.trancode });
            cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = this.PolisNo });
            cmd.Parameters.Add(new MySqlParameter("@IsSukses", MySqlDbType.Bit) { Value = this.IsSukses });
            cmd.Parameters.Add(new MySqlParameter("@ErrorMessage", MySqlDbType.VarChar) { Value = this.ErrorMessage });

            cmd.ExecuteNonQuery();
            con.CloseAsync();
        }

    }
}
