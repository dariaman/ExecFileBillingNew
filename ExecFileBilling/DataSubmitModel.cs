using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class DataSubmitModel
    {
        public int id { get; set; }
        public int freq_payment { get; set; }
        public string PolisNo { get; set; }
        public Decimal Amount { get; set; }
        public string ApprovalCode { get; set; }
        public string Deskripsi { get; set; }
        public string AccNo { get; set; }
        public string AccName { get; set; }
        public Boolean IsSukses { get; set; }
        public string PolisId { get; set; }
        public string BillingID { get; set; }
        public string BillCode { get; set; }
        //public string BillStatus { get; set; }
        //public string PolisStatus { get; set; }
        //public Decimal? PremiAmount { get; set; }
        //public Decimal? CashlessFeeAmount { get; set; }
        //public Decimal? TotalAmount { get; set; }

        // Data virtual
        public string GroupRejectMapping { get; set; }
        public string TransHistory { get; set; }
        public string receiptID { get; set; }
        public string receiptOtherID { get; set; }
        public string PolisNoteReceiptID { get; set; }
        public string PolisNoteReceiptOtherID { get; set; }
        public string TransID { get; set; } // ID dari CC/AC Transaction di Life21
    }
}
