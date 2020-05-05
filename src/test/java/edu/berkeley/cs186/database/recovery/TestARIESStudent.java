package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.HW5Tests;
import edu.berkeley.cs186.database.categories.StudentTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.DiskSpaceManagerImpl;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * File for student tests for HW5 (Recovery). Tests are run through
 * TestARIESStudentRunner for grading purposes.
 */
@Category({HW5Tests.class, StudentTests.class})
public class TestARIESStudent {
    private String testDir;
    private RecoveryManager recoveryManager;
    private final Queue<Consumer<LogRecord>> redoMethods = new ArrayDeque<>();

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        testDir = tempFolder.newFolder("test-dir").getAbsolutePath();
        recoveryManager = loadRecoveryManager(testDir);
        DummyTransaction.cleanupTransactions();
        LogRecord.onRedoHandler(t -> {});
    }

    @After
    public void cleanup() throws Exception {}

    @Test
    public void testStudentAnalysis() throws Exception {
        // TODO(hw5): write your own test on restartAnalysis only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runAnalysis(inner) instead of
        // inner.restartAnalysis() to call the analysis routine.
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);
        DummyTransaction transaction2 = DummyTransaction.create(2L);

        Map<Long, Long> dirtyPageTable = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> transactionTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord(9876543210L))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, LSNs.get(0), (short) 0, before,
                after))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(1), (short) 0, before,
                after))); // 2
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, LSNs.get(2)))); // 3
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(2L, 10000000001L, 0, (short) 0, before,
                after))); // 5

        dirtyPageTable.put(10000000001L, LSNs.get(1));
        dirtyPageTable.put(10000000002L, LSNs.get(2));
        transactionTable.put(1L, new Pair<>(Transaction.Status.RUNNING, LSNs.get(2)));
        touchedPages.put(1L, Arrays.asList(10000000001L, 10000000002L));

        LSNs.add(logManager.appendToLog(new EndCheckpointLogRecord(dirtyPageTable, transactionTable, touchedPages))); // 6

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // new recovery manager - tables/log manager/other state loaded with old manager are different
        // with the new recovery manager
        logManager = getLogManager(recoveryManager);
        dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> analysisTransactionTable = getTransactionTable(recoveryManager);
        List<String> lockRequests = getLockRequests(recoveryManager);

        runAnalysis(recoveryManager);

        // Xact table
        assertFalse(analysisTransactionTable.containsKey(1L));
        assertTrue(analysisTransactionTable.containsKey(2L));
        assertEquals(new HashSet<>(Collections.singletonList(10000000001L)),
                analysisTransactionTable.get(2L).touchedPages);

        // DPT
        assertTrue(dirtyPageTable.containsKey(10000000001L));
        assertTrue(dirtyPageTable.containsKey(10000000002L));
        assertEquals((long) LSNs.get(1), (long) dirtyPageTable.get(10000000001L));
        assertEquals((long) LSNs.get(2), (long) dirtyPageTable.get(10000000002L));

        // status/cleanup
        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertTrue(transaction1.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction2.getStatus());
        assertFalse(transaction2.cleanedUp);

        // lock requests made
        assertEquals(Arrays.asList(
                "request 1 X(database/1/10000000001)",
                "request 1 X(database/1/10000000002)",
                "request 2 X(database/1/10000000001)"
        ), lockRequests);

        // transaction counter - from begin checkpoint
        assertEquals(9876543210L, getTransactionCounter(recoveryManager));

        // FlushedLSN
        assertEquals(LogManagerImpl.maxLSN(LogManagerImpl.getLSNPage(LSNs.get(6))),
                logManager.getFlushedLSN());
    }

    @Test
    public void testStudentRedo() throws Exception {
        // TODO(hw5): write your own test on restartRedo only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runRedo(inner) instead of
        // inner.restartRedo() to call the analysis routine.
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before,
                after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1,
                after, before))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(1), (short) 2,
                before, after))); // 2
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(2)))); // 3
        LSNs.add(logManager.appendToLog(new UndoUpdatePageLogRecord(
                1L, 10000000003L, LSNs.get(3), LSNs.get(1), (short) 2, before))); // 4
        LSNs.add(logManager.appendToLog(new UndoUpdatePageLogRecord(
                1L, 10000000002L, LSNs.get(4), LSNs.get(0), (short) 1, after))); // 5

        // actually do the first and second write (and get it flushed to disk)
        logManager.fetchLogRecord(LSNs.get(0)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(dsm, bm);

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up dirty page table - xact table is empty (transaction ended)
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        dirtyPageTable.put(10000000001L, LSNs.get(0));
        dirtyPageTable.put(10000000002L, LSNs.get(1));
        dirtyPageTable.put(10000000003L, LSNs.get(2));

        // set up checks for redo - these get called in sequence with each LogRecord#redo call
        setupRedoChecks(Arrays.asList(
                (LogRecord record) -> assertEquals((long) LSNs.get(2), (long) record.LSN),
                (LogRecord record) -> assertEquals((long) LSNs.get(4), (long) record.LSN),
                (LogRecord record) -> assertEquals((long) LSNs.get(5), (long) record.LSN)
        ));

        runRedo(recoveryManager);

        finishRedoChecks();
    }

    @Test
    public void testStudentUndo() throws Exception {
        // TODO(hw5): write your own test on restartUndo only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runUndo(inner) instead of
        // inner.restartUndo() to call the analysis routine.
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);
        DummyTransaction transaction2 = DummyTransaction.create(2L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before,
                after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1,
                before, after))); // 1
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(1)))); // 2
        LSNs.add(logManager.appendToLog(new UndoUpdatePageLogRecord(
                1L, 10000000002L, LSNs.get(2), LSNs.get(0), (short) 1, before))); // 3
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(2L, 10000000003L, 0L, (short) 0, before,
                after))); // 4
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(2L, LSNs.get(4)))); // 5
        LSNs.add(logManager.appendToLog(new UndoUpdatePageLogRecord(
                2L, 10000000003L, LSNs.get(5), 0L, (short) 1, before))); // 6

        // actually do the writes
        for (int i = 0; i < 6; ++i) {
            LogRecord record = logManager.fetchLogRecord(LSNs.get(i));
            if (record.isRedoable())
                logManager.fetchLogRecord(LSNs.get(i)).redo(dsm, bm);
        }

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up xact table - leaving DPT empty
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        TransactionTableEntry entry1 = new TransactionTableEntry(transaction1);
        TransactionTableEntry entry2 = new TransactionTableEntry(transaction2);
        entry1.lastLSN = LSNs.get(3);
        entry1.touchedPages = new HashSet<>(Arrays.asList(10000000001L, 10000000002L));
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);
        entry2.lastLSN = LSNs.get(6);
        entry2.touchedPages = new HashSet<>(Collections.singletonList(10000000003L));
        entry2.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(2L, entry2);
        logManager = getLogManager(recoveryManager);

        // set up checks for undo - these get called in sequence with each LogRecord#redo call
        // (which should be called on CLRs)
        setupRedoChecks(Collections.singletonList(
                (LogRecord record) -> {
                    assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
                    assertNotNull("log record not appended to log yet", record.LSN);
                    assertFalse(transactionTable.containsKey(2L));
                    assertEquals(Optional.of(10000000001L), record.getPageNum());
                }
        ));

        runUndo(recoveryManager);

        finishRedoChecks();

        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());

        Iterator<LogRecord> iter = logManager.scanFrom(LSNs.get(6));
        iter.next();
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UNDO_UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testStudentIntegration() throws Exception {
        // TODO(hw5): write your own test on all of RecoveryManager
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager.
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);
        Transaction transaction3 = DummyTransaction.create(3L);
        recoveryManager.startTransaction(transaction3);

        List<Long> LSNs = new ArrayList<>();

        LSNs.add(recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after));  // 0
        recoveryManager.savepoint(1L, "Transaction1-1");
        LSNs.add(recoveryManager.logPageWrite(1L, 10000000002L, (short) 1, after, before));  // 1
        LSNs.add(recoveryManager.logPageWrite(2L, 10000000003L, (short) 2, before, after));  // 2
        recoveryManager.rollbackToSavepoint(1L, "Transaction1-1");
        LSNs.add(recoveryManager.logPageWrite(1L, 10000000002L, (short) 1, before, after));  // 3
        LSNs.add(recoveryManager.logPageWrite(3L, 10000000004L, (short) 0, before, after));  // 4
        LSNs.add(recoveryManager.commit(1L));  // 5
        LSNs.add(recoveryManager.logPageWrite(3L, 10000000005L, (short) 3, after, before));  // 6
        LSNs.add(recoveryManager.abort(3L));  // 7

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        recoveryManager.restart();

        assertFalse(transactionTable.containsKey(1L));
        assertTrue(transactionTable.containsKey(2L));
        assertTrue(transactionTable.containsKey(3L));

        Iterator<LogRecord> iter = logManager.iterator();
        assertEquals(LogType.MASTER, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UNDO_UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.COMMIT_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, iter.next().getType());

        LogRecord record = iter.next();
        assertEquals(LogType.END_TRANSACTION, record.getType());
        assertEquals(Optional.of(1L), record.getTransNum());

        record = iter.next();
        assertEquals(LogType.ABORT_TRANSACTION, record.getType());
        assertEquals(Optional.of(2L), record.getTransNum());

        assertFalse(iter.hasNext());

        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);
        transactionTable = getTransactionTable(recoveryManager);

        redoMethods.clear();
        setupRedoChecks(Collections.emptyList());

        Runnable undo = recoveryManager.restart(); // run analysis and redo in restart recovery

        finishRedoChecks(); // redo shouldn't call
        assertFalse(transactionTable.containsKey(1L));
        assertTrue(transactionTable.containsKey(2L));
        assertTrue(transactionTable.containsKey(3L));

        undo.run();

        assertFalse(transactionTable.containsKey(1L));
        assertFalse(transactionTable.containsKey(2L));
        assertFalse(transactionTable.containsKey(3L));
    }

    // TODO(hw5): add as many (ungraded) tests as you want for testing!

    @Test
    public void testCase() throws Exception {
        // TODO(hw5): write your own test! (ungraded)
    }

    @Test
    public void anotherTestCase() throws Exception {
        // TODO(hw5): write your own test!!! (ungraded)
    }

    @Test
    public void yetAnotherTestCase() throws Exception {
        // TODO(hw5): write your own test!!!!! (ungraded)
    }

    /*************************************************************************
     * Helpers for writing tests.                                            *
     * Do not change the signature of any of the following methods.          *
     *************************************************************************/

    /**
     * Helper to set up checks for redo. The first call to LogRecord.redo will
     * call the first method in METHODS, the second call to the second method in METHODS,
     * and so on. Call this method before the redo pass, and call finishRedoChecks
     * after the redo pass.
     */
    private void setupRedoChecks(Collection<Consumer<LogRecord>> methods) {
        for (final Consumer<LogRecord> method : methods) {
            redoMethods.add(record -> {
                method.accept(record);
                LogRecord.onRedoHandler(redoMethods.poll());
            });
        }
        redoMethods.add(record -> {
            fail("LogRecord#redo() called too many times");
        });
        LogRecord.onRedoHandler(redoMethods.poll());
    }

    /**
     * Helper to finish checks for redo. Call this after the redo pass (or undo pass)-
     * if not enough redo calls were performed, an error is thrown.
     *
     * If setupRedoChecks is used for the redo pass, and this method is not called before
     * the undo pass, and the undo pass calls undo at least once, an error may be incorrectly thrown.
     */
    private void finishRedoChecks() {
        assertTrue("LogRecord#redo() not called enough times", redoMethods.isEmpty());
        LogRecord.onRedoHandler(record -> {});
    }

    /**
     * Loads the recovery manager from disk.
     * @param dir testDir
     * @return recovery manager, loaded from disk
     */
    protected RecoveryManager loadRecoveryManager(String dir) throws Exception {
        RecoveryManager recoveryManager = new ARIESRecoveryManagerNoLocking(
            new DummyLockContext(new Pair<>("database", 0L)),
            DummyTransaction::create
        );
        DiskSpaceManager diskSpaceManager = new DiskSpaceManagerImpl(dir, recoveryManager);
        BufferManager bufferManager = new BufferManagerImpl(diskSpaceManager, recoveryManager, 32,
                new LRUEvictionPolicy());
        boolean isLoaded = true;
        try {
            diskSpaceManager.allocPart(0);
            diskSpaceManager.allocPart(1);
            for (int i = 0; i < 10; ++i) {
                diskSpaceManager.allocPage(DiskSpaceManager.getVirtualPageNum(1, i));
            }
            isLoaded = false;
        } catch (IllegalStateException e) {
            // already loaded
        }
        recoveryManager.setManagers(diskSpaceManager, bufferManager);
        if (!isLoaded) {
            recoveryManager.initialize();
        }
        return recoveryManager;
    }

    /**
     * Flushes everything to disk, but does not call RecoveryManager#shutdown. Similar
     * to pulling the plug on the database at a time when no changes are in memory. You
     * can simulate a shutdown where certain changes _are_ in memory, by simply never
     * applying them (i.e. write a log record, but do not make the changes on the
     * buffer manager/disk space manager).
     */
    protected void shutdownRecoveryManager(RecoveryManager recoveryManager) throws Exception {
        ARIESRecoveryManager arm = (ARIESRecoveryManager) recoveryManager;
        arm.logManager.close();
        arm.bufferManager.evictAll();
        arm.bufferManager.close();
        arm.diskSpaceManager.close();
        DummyTransaction.cleanupTransactions();
    }

    protected BufferManager getBufferManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).bufferManager;
    }

    protected DiskSpaceManager getDiskSpaceManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).diskSpaceManager;
    }

    protected LogManager getLogManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).logManager;
    }

    protected List<String> getLockRequests(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).lockRequests;
    }

    protected long getTransactionCounter(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManagerNoLocking) recoveryManager).transactionCounter;
    }

    protected Map<Long, Long> getDirtyPageTable(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).dirtyPageTable;
    }

    protected Map<Long, TransactionTableEntry> getTransactionTable(RecoveryManager recoveryManager)
    throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).transactionTable;
    }

    protected void runAnalysis(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartAnalysis();
    }

    protected void runRedo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartRedo();
    }

    protected void runUndo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartUndo();
    }
}
