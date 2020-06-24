using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Log
{
    internal static class LoggingExtensions
    {
        internal static string SafeSubstring(this string str, int maxChars)
        {
            if (string.IsNullOrEmpty(str))
            {
                return string.Empty;
            }

            return str.Substring(0, Math.Min(maxChars, str.Length));
        }
    }
}
