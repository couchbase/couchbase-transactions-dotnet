namespace Couchbase.Transactions
{
    public enum AttemptState
    {
        NotStarted,
        PEnding,
        Aborted,
        Committed,
        Completed,
        RolledBack
    }
}