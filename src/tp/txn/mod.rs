mod txn;

pub(crate) enum Commands {
    Prewrite,
    Commit,
    Cleanup,
    Rollback,
    ResolveLock,
}