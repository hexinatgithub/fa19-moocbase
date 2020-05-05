package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.lang.reflect.Parameter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(hw5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        LogRecord commitRecord = new CommitTransactionLogRecord(
                transNum, transactionEntry.lastLSN);
        long LSN = logManager.appendToLog(commitRecord);
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        transactionEntry.lastLSN = LSN;
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(hw5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        LogRecord abortRecord = new AbortTransactionLogRecord(
                transNum, transactionEntry.lastLSN);
        long LSN = logManager.appendToLog(abortRecord);
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);
        transactionEntry.lastLSN = LSN;
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(hw5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // undone change if transaction is aborting
        if (transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING &&
                transactionEntry.lastLSN != 0) {
            rollbackToTheStart(transNum);
        }

        LogRecord endRecord = new EndTransactionLogRecord(transNum, transactionEntry.lastLSN);
        long LSN = logManager.appendToLog(endRecord);
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);
        return LSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(hw5): implement
        long LSN;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);

        if (before.length * 2 > (BufferManager.EFFECTIVE_PAGE_SIZE / 2)) {
            LogRecord undoOnlyRecord = new UpdatePageLogRecord(
                    transNum, pageNum, transactionEntry.lastLSN, pageOffset, before, null);
            LSN = logManager.appendToLog(undoOnlyRecord);
            LogRecord redoOnlyRecord = new UpdatePageLogRecord(
                    transNum, pageNum, LSN, pageOffset, null, after);
            LSN = logManager.appendToLog(redoOnlyRecord);
        } else {
            LogRecord record = new UpdatePageLogRecord(
                    transNum, pageNum, transactionEntry.lastLSN, pageOffset, before, after);
            LSN = logManager.appendToLog(record);
        }

        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        if (!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, LSN);
        }
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(hw5): implement
        rollbackTo(transNum, LSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(hw5): generate end checkpoint record(s) for DPT and transaction table
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size() + 1, txnTable.size(), touchedPages.size(), numTouchedPages);

            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }

            dpt.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry transactionEntry = entry.getValue();
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size(), txnTable.size() + 1, touchedPages.size(), numTouchedPages);

            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }

            txnTable.put(transNum, new Pair<>(
                    transactionEntry.transaction.getStatus(), transactionEntry.lastLSN));
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(hw5): add any helper methods needed

    // rollback to the transaction first log record, inclusive first log
    // record.
    private void rollbackToTheStart(long tranNum) {
        rollbackTo(tranNum, 0);
    }

    /**
     * rollback to given LSN log record, inclusive the given log record.
     * @param tranNum transaction number
     * @param LSN of log record to rollback to, 0 rollback to
     * the first log record of transaction.
     */
    private void rollbackTo(long tranNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(tranNum);
        LogRecord nextUndoRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);

        Pair<LogRecord, Boolean> undoP;
        LogRecord tmpCLR;
        Optional<Long> opt;

        while (nextUndoRecord != null &&
                nextUndoRecord.getLSN() > LSN) {
            if (nextUndoRecord.isUndoable()) {
                undoP = nextUndoRecord.undo(transactionEntry.lastLSN);
                tmpCLR = undoP.getFirst();
                transactionEntry.lastLSN = logManager.appendToLog(tmpCLR);
                if (undoP.getSecond()) {
                    logManager.flushToLSN(transactionEntry.lastLSN);
                }
                tmpCLR.redo(diskSpaceManager, bufferManager);

                opt = nextUndoRecord.getType() == LogType.UNDO_UPDATE_PAGE ?
                        nextUndoRecord.getUndoNextLSN() : nextUndoRecord.getPrevLSN();
                nextUndoRecord = opt.map(logManager::fetchLogRecord).
                        orElse(null);
                continue;
            }
            nextUndoRecord = nextUndoRecord.getPrevLSN().
                    map(logManager::fetchLogRecord).orElse(null);
        }
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(hw5): implement
        restartAnalysis();
        restartRedo();

        return () -> {
            // clean dirty page
            restartUndo();
            checkpoint();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(hw5): implement
        Iterator<LogRecord> it = logManager.scanFrom(LSN);
        while (it.hasNext()) {
            record = it.next();
            switch (record.getType()) {
                // checkpoint records
                case BEGIN_CHECKPOINT:
                    updateTransactionCounter.accept(record.getMaxTransactionNum().
                            orElse(0L));
                    break;
                case END_CHECKPOINT:
                    analysisCheckoutPointStatusRestore(record);
                    break;
                // log records for transaction status changes
                case COMMIT_TRANSACTION:
                case ABORT_TRANSACTION:
                case END_TRANSACTION:
                    analysisTransactionStatusHandler(record);
                    break;
                // log record for operations that a transaction does
                case FREE_PART:
                case ALLOC_PART:
                case UNDO_FREE_PART:
                case UNDO_ALLOC_PART:
                    analysisTransactionOperationHandler(record, false, false);
                    break;
                case ALLOC_PAGE:
                case FREE_PAGE:
                case UNDO_ALLOC_PAGE:
                case UNDO_FREE_PAGE:
                    analysisTransactionOperationHandler(record, true, true);
                    break;
                case UPDATE_PAGE:
                case UNDO_UPDATE_PAGE:
                    analysisTransactionOperationHandler(record, true, false);
                    break;
            }
        }

        List<Long> committingTransaction = new ArrayList<>();
        List<Long> loserTransaction = new ArrayList<>();

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry transactionEntry = entry.getValue();
            if (transactionEntry.transaction.getStatus() == Transaction.Status.COMMITTING)
                committingTransaction.add(transNum);
            else if (transactionEntry.transaction.getStatus() == Transaction.Status.RUNNING)
                loserTransaction.add(transNum);
        }

        for (Long transNum : committingTransaction) {
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            transactionEntry.lastLSN = logManager.appendToLog(
                    new EndTransactionLogRecord(transNum, transactionEntry.lastLSN));
            transactionEntry.transaction.cleanup();
            transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
            transactionTable.remove(transNum);
        }

        for (Long transNum : loserTransaction) {
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            transactionEntry.lastLSN = logManager.appendToLog(
                    new AbortTransactionLogRecord(transNum, transactionEntry.lastLSN));
            transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(hw5): implement
        long minRecLSH = Long.MAX_VALUE;
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            if (entry.getValue() < minRecLSH) {
                minRecLSH = entry.getValue();
            }
        }

        LogRecord record;
        Iterator<LogRecord> it = logManager.scanFrom(minRecLSH);
        while (it.hasNext()) {
            record = it.next();
            if (record.isRedoable()) {
                switch (record.getType()) {
                    case FREE_PART:
                    case ALLOC_PART:
                    case UNDO_FREE_PART:
                    case UNDO_ALLOC_PART:
                        record.redo(diskSpaceManager, bufferManager);
                        break;
                    case ALLOC_PAGE:
                    case FREE_PAGE:
                    case UNDO_ALLOC_PAGE:
                    case UNDO_FREE_PAGE:
                    case UPDATE_PAGE:
                    case UNDO_UPDATE_PAGE:
                        Optional<Long> optional = record.getPageNum();
                        assert (optional.isPresent());
                        long pageNum = optional.get();

                        if (!dirtyPageTable.containsKey(pageNum)) {
                            continue;
                        }

                        if (record.getLSN() < dirtyPageTable.get(pageNum)) {
                           continue;
                        }

                        Page page = bufferManager.fetchPage(dbContext, pageNum, false);
                        try {
                            if (page.getPageLSN() < record.getLSN()) {
                                record.redo(diskSpaceManager, bufferManager);
                            }
                        } finally {
                            page.unpin();
                        }
                        break;
                    default:
                }
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(hw5): implement
        PriorityQueue<Long> pq = new PriorityQueue<>(
                transactionTable.size(), Collections.reverseOrder());
        for (TransactionTableEntry entry : transactionTable.values()) {
            if (entry.transaction.getStatus() ==
                    Transaction.Status.RECOVERY_ABORTING)
                pq.add(entry.lastLSN);
        }

        long transNum, pageNum, LSN, nextLSN;
        Optional<Long> optional;
        LogRecord record, tmpCLR;
        Pair<LogRecord, Boolean> undoP;
        TransactionTableEntry transactionEntry;
        while (!pq.isEmpty()) {
            LSN = pq.remove();
            record = logManager.fetchLogRecord(LSN);
            optional = record.getTransNum();
            assert (optional.isPresent());
            transNum = optional.get();
            transactionEntry = transactionTable.get(transNum);

            if (record.isUndoable()) {
                optional = record.getPageNum();
                assert (optional.isPresent());
                pageNum = optional.get();

                undoP = record.undo(transactionEntry.lastLSN);
                tmpCLR = undoP.getFirst();
                LSN = logManager.appendToLog(tmpCLR);
                transactionEntry.lastLSN = LSN;
                if (undoP.getSecond()) {
                    logManager.flushToLSN(tmpCLR.LSN);
                }
                tmpCLR.redo(diskSpaceManager, bufferManager);
                dirtyPageTable.putIfAbsent(pageNum, LSN);

                nextLSN = tmpCLR.getUndoNextLSN().orElse(
                        record.getPrevLSN().orElse(0L));
            } else
                nextLSN = record.getUndoNextLSN().orElse(
                        record.getPrevLSN().orElse(0L));

            if (nextLSN == 0) {
                LSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, LSN));
                transactionEntry.lastLSN = LSN;
                transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
                continue;
            }
            pq.add(nextLSN);
        }
    }

    // TODO(hw5): add any helper methods needed

    private void analysisCheckoutPointStatusRestore(LogRecord record) {
        assert (record.getType() == LogType.END_CHECKPOINT);
        EndCheckpointLogRecord endCheckpointRecord =
                (EndCheckpointLogRecord) record;
        Map<Long, Long> cpDirtyPageTable =
                endCheckpointRecord.getDirtyPageTable();
        Map<Long, Pair<Transaction.Status, Long>> cpTransactionTable =
                endCheckpointRecord.getTransactionTable();
        Map<Long, List<Long>> cpTouchedPages =
                endCheckpointRecord.getTransactionTouchedPages();

        for (Map.Entry<Long, Long> entry : cpDirtyPageTable.entrySet()) {
            long pageNum = entry.getKey();
            long recLSN = entry.getValue();
            dirtyPageTable.put(pageNum, recLSN);
        }

        for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : cpTransactionTable.entrySet()) {
            long transNum = entry.getKey();
            long lastLSN = entry.getValue().getSecond();
            Transaction.Status status = entry.getValue().getFirst();
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);

            if (transactionEntry != null && lastLSN >= transactionEntry.lastLSN) {
                transactionEntry.lastLSN = lastLSN;
                transactionEntry.transaction.setStatus(status);
            }
        }

        for (Map.Entry<Long, List<Long>> entry : cpTouchedPages.entrySet()) {
            long transNum = entry.getKey();
            List<Long> touchedPages = entry.getValue();
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);

            if (transactionEntry != null &&
                    transactionEntry.transaction.getStatus() != Transaction.Status.COMPLETE) {
                touchedPages.forEach(pageNum -> {
                    transactionEntry.touchedPages.add(pageNum);
                    acquireTransactionLock(transactionEntry.transaction,
                            getPageLockContext(pageNum), LockType.X);
                });
            }
        }
    }

    private void analysisTransactionStatusHandler(LogRecord record) {
        long transNum;
        Optional<Long> transNumOptional;
        TransactionTableEntry transactionEntry;

        transNumOptional = record.getTransNum();
        assert (transNumOptional.isPresent());
        transNum = transNumOptional.get();
        transactionEntry = transactionTable.get(transNum);
        transactionEntry.lastLSN = record.getLSN();

        switch (record.getType()) {
            case COMMIT_TRANSACTION:
                transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
                break;
            case ABORT_TRANSACTION:
                transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                break;
            case END_TRANSACTION:
                transactionEntry.transaction.cleanup();
                transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
                break;
        }
    }

    private void analysisTransactionOperationHandler(LogRecord record,
                                                     boolean page_related, boolean flushed) {
        long transNum, pageNum;
        TransactionTableEntry transactionEntry;
        Optional<Long> transNumOptional, pageNumOptional;

        transNumOptional = record.getTransNum();
        assert (transNumOptional.isPresent());
        transNum = transNumOptional.get();
        pageNumOptional = record.getPageNum();
        assert (pageNumOptional.isPresent());
        pageNum = pageNumOptional.get();

        transactionTable.putIfAbsent(transNum,
                new TransactionTableEntry(newTransaction.apply(transNum)));
        transactionEntry = transactionTable.get(transNum);
        transactionEntry.lastLSN = record.getLSN();

        if (page_related) {
            transactionEntry.touchedPages.add(pageNum);
            acquireTransactionLock(transactionEntry.transaction,
                    getPageLockContext(pageNum), LockType.X);
        }

        if (flushed)
            dirtyPageTable.remove(pageNum);
        else
            dirtyPageTable.putIfAbsent(pageNum, record.getLSN());
    }

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
