using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class DataUploadModel
    {
        public string PolisNo { get; set; }

        public DateTime? TglPaid { get; set; }
        public Decimal Amount { get; set; }
        public string ApprovalCode { get; set; }
        public string Deskripsi { get; set; }
        public string AccNo { get; set; }
        public string AccName { get; set; }
        public Boolean IsSukses{ get; set; }
        public Boolean IsExec { get; set; }
    }
}
