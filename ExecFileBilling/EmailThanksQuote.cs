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
    class EmailThanksQuote
    {
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];
        public string BCCTo = ConfigurationManager.AppSettings["EmailPHS"];

        private int BillingID { get; set; }        
        private string SendTo { get; set; }
        private string SubjectEmail { get; set; }
        private string BodyEmail { get; set; }
        private string CetakPolis { get; set; }
        private DateTime Tgl { get; set; }
        private Decimal Amount { get; set; }

        public EmailThanksQuote(int billingID,  Decimal jlhBayar,DateTime? tgl)
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
SELECT qb.`Holder_Name`, q.`IsLaki`,q.`email`,p.`product_description`,qb.`ref_no`,qb.`cashless_fee`,q.`POB`,q.`DOB`,
q.`mobile_phone`,q.`sum_insured`,q.`duration`,q.`duration_days`,qb.`prospect_amount`,q.`premium_mode`,q.`payment_method`
FROM `quote_billing` qb
INNER JOIN `quote` q ON q.`quote_id`=qb.`quote_id`
INNER JOIN `product` p ON p.`product_id`=qb.`product_id`
WHERE qb.`quote_id`=@billID;", con)
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
                        this.SubjectEmail = string.Format(@"JAGADIRI: Nomor Quotation: {0} TERBAYAR", rd["ref_no"].ToString());
                        this.CetakPolis = string.Format(@"<tr><td>Biaya Cetak Polis</td>  <td>: IDR {0}</td></tr>", Convert.ToDecimal(rd["cashless_fee"]).ToString("#,###"));
                        this.BodyEmail = string.Format(@"Dengan Hormat {0} {1},
<p style='text-align:justify'>Terima kasih atas Pembayaran Asuransi Anda. Permohonan Asuransi Anda akan segera kami proses dan kami akan informasikan Anda kembali via email </p>
<table>
<tr><td></td><td></td></tr>
    <tr><td>No Quote</td>               <td>: {2}</td></tr>
    <tr><td>Nama</td>                   <td>: {1}</td></tr>
    <tr><td>Jenis Kelamin</td>          <td>: {3}</td></tr>
    <tr><td>Tempat/Tanggal Lahir</td>   <td>: {4}/{5}</td></tr>
    <tr><td>Email</td>                  <td>: {6}</td></tr>
    <tr><td>Mobile</td>                 <td>: {7}</td></tr>
    <tr><td>Nama Product</td>           <td>: {8}</td></tr>
    <tr><td>Uang Pertanggungan</td>     <td>: IDR {9}</td></tr>
    <tr><td>Durasi (tahun)</td>         <td>: {10} tahun</td></tr>
    <tr><td>Durasi (hari)</td>          <td>: {11} hari</td></tr>
    <tr><td>Total Premi</td>            <td>: IDR {12}</td></tr>" +
    this.CetakPolis +
    @"<tr><td>Frekuensi Bayar</td>        <td>: {13}</td></tr>
<tr><td><br></td><td></td></tr>
    <tr><td>Pembayaran</td><td></td></tr>
    <tr><td>Metode Pembayaran</td>      <td>: {14}</td></tr>
    <tr><td>Jumlah Pembayaran</td>      <td>: IDR {15}</td></tr>
    <tr><td>Status</td>                 <td>: TERDAFTAR</td></tr>
</table>
<br><br>Sukses selalu,
<br>JAGADIRI ",
(Convert.ToBoolean(rd["IsLaki"]) ? "Bapak" : "Ibu"),
rd["Holder_Name"].ToString().ToUpper(),
rd["ref_no"].ToString(),
(Convert.ToBoolean(rd["IsLaki"]) ? "Pria" : "Wanita"),
rd["POB"].ToString(),
rd["DOB"].ToString() == string.Empty ? "" : Convert.ToDateTime(rd["DOB"]).ToString("dd MMM yyyy"),
rd["email"].ToString(),
rd["mobile_phone"].ToString(),
rd["product_description"].ToString(),
rd["sum_insured"].ToString() == string.Empty ? "" : Convert.ToDecimal(rd["sum_insured"]).ToString("#,###"),
rd["duration"].ToString(),
rd["duration_days"].ToString(),
rd["prospect_amount"].ToString() == string.Empty ? "" : Convert.ToDecimal(rd["prospect_amount"]).ToString("#,###"),
rd["premium_mode"].ToString() == string.Empty ? "Sekaligus" : 
    (rd["premium_mode"].ToString() == "0" ? "Sekaligus" : 
        (rd["premium_mode"].ToString() == "1" ? "Bulanan" : 
            (rd["premium_mode"].ToString() == "3" ? "Triwulanan" : 
                (rd["premium_mode"].ToString() == "6" ? "Semesteran" : 
                    (rd["premium_mode"].ToString() == "12" ? "Tahunan" : "-"))))),
rd["payment_method"].ToString(),
this.Amount.ToString("#,###"));

                        this.SendTo = rd["email"].ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("EmailThanksQuote initialEmail() : " + ex.Message);
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
                throw new Exception("EmailThanksQuote InsertEmailQuee() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }
    }
}
