using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class FileResultModel
    {
        public int Id { get; set; }
        public string trancode { get; set; }
        public string FileBilling { get; set; }
        public string FileName { get; set; }
        public string stageTable { get; set; }
        //public string FileSaveName { get; set; }
        public string source { get; set; }
        public DateTime tglProses { get; set; }
        public DateTime tglSkrg { get; set; }
        public int bankid_receipt { get; set; }
        public int bankid { get; set; }
        public int id_billing_download { get; set; }
        public string deskripsi { get; set; }
    }
}
