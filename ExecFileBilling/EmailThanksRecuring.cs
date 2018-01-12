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
    class EmailThanksRecuring
    {
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];
        public string BCCTo = ConfigurationManager.AppSettings["EmailPHS"];

        private int BillingID { get; set; }        
        private string SendTo { get; set; }
        private string SubjectEmail { get; set; }
        private string BodyEmail { get; set; }
        private DateTime Tgl { get; set; }
        private Decimal Amount { get; set; }

        public EmailThanksRecuring(int billingID, Decimal jlhBayar,DateTime? tgl)
        {
            BillingID = billingID;
            Tgl = tgl ?? DateTime.Now;
            Amount = jlhBayar;
            initialEmail(this.BillingID);
        }

        private void initialEmail(int billID)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"
SELECT pd.`product_description`,pb.`policy_no`,ci.`CustomerName`,ci.`IsLaki`,ci.`Email`
FROM `billing` b
INNER JOIN `policy_billing` pb ON pb.`policy_Id`=b.`policy_id`
INNER JOIN `product` pd ON pd.`product_id`=pb.`product_id`
INNER JOIN `customer_info` ci ON ci.`CustomerId`=pb.`holder_id`
WHERE b.`BillingID`=@billID", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Add(new MySqlParameter("@billID", MySqlDbType.Int32) { Value = this.BillingID });
            try
            {
                con.Open();
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        this.SubjectEmail = string.Format(@"JAGADIRI: Penerimaan Premi Regular {0} {1} {2}", 
                            rd["product_description"].ToString(), rd["policy_no"].ToString(), rd["CustomerName"].ToString().ToUpper());
                        this.BodyEmail = string.Format(@"Salam hangat {0} {1},<br>
<p style='text-align:justify'>Bersama surat ini kami ingin mengucapkan terima kasih atas pembayaran Premi Regular untuk Polis {2} dengan nomor polis {3} sejumlah IDR {4} yang telah kami terima. Pembayaran Premi tersebut secara otomatis akan membuat Polis Asuransi Anda tetap aktif dan memberikan manfaat perlindungan maksimal bagi Anda dan keluarga.</p>
<br>Sukses selalu,<br><br>JAGADIRI ", (Convert.ToBoolean(rd["IsLaki"]) ? "Bapak" : "Ibu"), rd["CustomerName"].ToString().ToUpper(), 
rd["product_description"].ToString(), rd["policy_no"].ToString(), this.Amount.ToString("#,###"));
                        this.SendTo = rd["Email"].ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("EmailThanksRecuring initialEmail() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public void InsertEmailQuee()
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"
INSERT INTO `prod_life21`.`system_email_queue`(`email_to`, `email_subject`, `email_body`, `email_created_dt`, `email_status`, `email_bcc`, `email_type`)
VALUES (@email_to, @email_subject, @email_body, @tgl, 'P', @email_bcc, 'UploadCCResult');", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Add(new MySqlParameter("@email_to", MySqlDbType.VarChar) { Value = this.SendTo });
            cmd.Parameters.Add(new MySqlParameter("@email_subject", MySqlDbType.VarChar) { Value = this.SubjectEmail });
            cmd.Parameters.Add(new MySqlParameter("@email_body", MySqlDbType.VarChar) { Value = this.BodyEmail });
            cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = this.Tgl });
            cmd.Parameters.Add(new MySqlParameter("@email_bcc", MySqlDbType.VarChar) { Value = this.BCCTo });
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
                //Console.Write(" Email Send >>>");
            }
            catch (Exception ex)
            {
                //Console.Write(" ... E R R O R");
                throw new Exception("EmailThanksRecuring InsertEmailQuee() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

    }
}
