using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error
{
    public enum ErrorClass
    {
        Undefined = 0,
        FailTransient = 1,
        FailHard = 2,
        FailOther = 3,
        FailAmbiguous = 4,
        FailDocAlreadyExists = 5,
        FailDocNotFound = 6,
        FailPathAlreadyExists = 7,
        FailPathNotFound = 8,
        FailCasMismatch = 9,
        FailExpiry = 10,
        FailWriteWriteConflict = 11,
        FailAtrFull = 12
    }
}
