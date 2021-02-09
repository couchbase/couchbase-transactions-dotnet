using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Couchbase.Transactions.DataModel
{
    internal record ParsedHLC(string now, string mode)
    {
        [JsonIgnore]
        public DateTimeOffset NowTime => DateTimeOffset.FromUnixTimeSeconds(int.Parse(now));

        [JsonIgnore]
        public CasMode CasModeParsed => mode switch {
                "l" => CasMode.LOGICAL,
                "logical" => CasMode.LOGICAL,
                "r" => CasMode.REAL,
                "real" => CasMode.REAL,
                _ => CasMode.UNKNOWN
            };

        internal enum CasMode
        {
            UNKNOWN = 0,
            REAL = 'r',
            LOGICAL = 'l'
        }
    }
}
