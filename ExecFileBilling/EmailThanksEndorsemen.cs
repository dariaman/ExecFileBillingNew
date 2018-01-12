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
    class EmailThanksEndorsemen
    {
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];
        public string BCCTo = ConfigurationManager.AppSettings["EmailPHS"];

        private string BillingID { get; set; }
        private string SendTo { get; set; }
        private string SubjectEmail { get; set; }
        private string BodyEmail { get; set; }
        private string CetakPolis { get; set; }
        private DateTime Tgl { get; set; }
        private Decimal Amount { get; set; }

        public EmailThanksEndorsemen(string billingID, Decimal jlhBayar, DateTime? tgl)
        {
            this.BillingID = billingID;
            this.Tgl = tgl ?? DateTime.Now;
            this.Amount = jlhBayar;
            initialEmail(this.BillingID);
        }

        private void initialEmail(string billID)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"
SELECT pb.`policy_no`,ci.`CustomerName`,ci.`IsLaki`,pd.`product_description`, bo.`TotalAmount`,ci.`Email`,
CASE WHEN bo.`BillingType`='A2' THEN 'Endorsemen Cetak Polis Fisik'
WHEN bo.`BillingType`='A3' THEN 'Cetak Kartu' END AS 'ProductType'
FROM `billing_others` bo
INNER JOIN `policy_billing` pb ON pb.`policy_Id`=bo.`policy_id`
INNER JOIN `customer_info` ci ON ci.`CustomerId`=pb.`holder_id`
INNER JOIN `product` pd ON pd.`product_id`=pb.`product_id`
WHERE bo.`BillingID`=@billID;", con)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.Add(new MySqlParameter("@billID", MySqlDbType.VarChar) { Value = this.BillingID });
            try
            {
                con.Open();
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        this.SubjectEmail = string.Format(@"JAGADIRI: Pembayaran {0}", rd["ProductType"].ToString());
                        this.BodyEmail = string.Format(@"Salam hangat {0} {1},<br>
<p style='text-align:justify'>Bersama surat ini kami ingin mengucapkan terima kasih atas pembayaran {2} untuk Polis {3} 
dengan nomor polis {4} sejumlah {5} yang telah kami terima. Pembayaran Premi tersebut secara otomatis akan membuat Polis Asuransi Anda tetap aktif dan memberikan manfaat perlindungan maksimal bagi Anda dan keluarga.</p>
<br>Sukses selalu,
<br>JAGADIRI ",
(Convert.ToBoolean(rd["IsLaki"]) ? "Bapak" : "Ibu"), rd["CustomerName"].ToString().ToUpper(), rd["ProductType"].ToString(),
rd["product_description"].ToString(), rd["policy_no"].ToString(),this.Amount.ToString("#,###"));

                        this.SendTo = rd["email"].ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("EmailThanksEndorsemen initialEmail : " + ex.Message);
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
            }
            catch (Exception ex)
            {
                throw new Exception("EmailThanksEndorsemen InsertEmailQuee : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }
    }
}
