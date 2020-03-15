package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // TODO(hw4_part1): You may add helper methods here if you wish
        /**
         * @return Current lock type the resource locked.
         */
        public LockType getLockType() {
            if (locks.isEmpty()) {
                return LockType.NL;
            }
            return locks.get(0).lockType;
        }

        /**
         * @return true if new lock can be grant immediately, TRANSACTION may already have a
         * lock on the entry so don't conflict with itself.
         */
        public boolean compatibleWithOthers(TransactionContext transaction, LockType lockType) {
            for (Lock lock: locks) {
                if (lock.transactionNum != transaction.getTransNum() &&
                        !LockType.compatible(lock.lockType, lockType))
                    return false;
            }
            return true;
        }

        /**
         * @return true if promote can be grant immediately.
         */
        public boolean compatiblePromote(TransactionContext transaction, LockType newLockType) {
            return compatibleWithOthers(transaction, newLockType) &&
                    locks.size() == 1 && transaction.getTransNum() == locks.get(0).transactionNum;
        }

        /**
         * @return true if lock can be grant immediately.
         */
        public boolean compatible(LockType lockType) {
            return LockType.compatible(getLockType(), lockType) &&
                    waitingQueue.isEmpty();
        }

        /**
         * promote lock from old lock type to new lock type.
         */
        public void promote(TransactionContext transaction, LockType newLockType) {
            assert locks.size() == 1;
            assert transaction.getTransNum() == locks.get(0).transactionNum;
            locks.get(0).lockType = newLockType;
        }

        public void release(Lock lock)
        throws NoLockHeldException {
            if (!locks.remove(lock)) {
                throw new NoLockHeldException(String.format(
                        "A lock on %s is not held by transaction %d.",
                        lock.name, lock.transactionNum));
            }

            LockRequest request; long txcNum;
            Lock tmp;
            while (!waitingQueue.isEmpty()) {
                request = waitingQueue.getFirst();
                if (!compatibleWithOthers(request.transaction, request.lock.lockType)) break;
                txcNum = request.transaction.getTransNum();
                // transaction may already held a lock on the resource, clear old lock
                // before add a new lock.
                for (Lock tmpLock: request.releasedLocks) {
                    LockManager.this.release(request.transaction, tmpLock.name);
                }

                if (compatiblePromote(request.transaction, request.lock.lockType)) {
                    tmp = locks.get(0);
                    LockManager.this.removeLockFromTransaction(txcNum, tmp);
                    locks.clear();
                }
                LockManager.this.addLockToTransaction(txcNum, request.lock);
                locks.add(request.lock);
                waitingQueue.removeFirst();
                request.transaction.unblock();
            }
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(hw4_part1): You may add helper methods here if you wish
    private void addLockToTransaction(long transactionNum, Lock lock) {
        getTransactionLocks(transactionNum).add(lock);
    }

    private void removeLockFromTransaction(long transactionNum, Lock lock) {
        List<Lock> locks = getTransactionLocks(transactionNum);
        locks.remove(lock);
        if (locks.isEmpty()) transactionLocks.remove(transactionNum);
    }

    private void promoteTransactionLock(long transactionNum, Lock lock, LockType newLockType) {
        List<Lock> locks = getTransactionLocks(transactionNum);
        int i = locks.indexOf(lock);
        locks.get(i).lockType = newLockType;
    }

    private List<Lock> getTransactionLocks(long transactionNum) {
        transactionLocks.putIfAbsent(transactionNum, new ArrayList<>());
        return transactionLocks.get(transactionNum);
    }

    /**
     * Error checking must be done before call. Only called by acquireAndRelease method.
     * @return list of Locks in RELEASELOCKS the TRANSACTION want to release.
     */
    private List<Lock> txcReleaseLocks(TransactionContext transaction,
                                       List<ResourceName> releaseLocks) {
        List<Lock> result = new ArrayList<>(releaseLocks.size());
        for (ResourceName name: releaseLocks) {
            result.add(new Lock(name, getLockType(transaction, name),
                    transaction.getTransNum()));
        }
        return result;
    }

    /**
     * Error checking must be done before call. Only called by acquireAndRelease method.
     * Release locks in RELEASELOCKS that TRANSACTION hold.
     */
    private void release(TransactionContext transaction,
                         List<ResourceName> releaseLocks) {
        for (ResourceName name: releaseLocks) {
            release(transaction, name);
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        ResourceEntry entry; Lock lock;
        long txcNum = transaction.getTransNum();
        boolean granted = false;
        synchronized (this) {
            if (getLockType(transaction, name) != LockType.NL &&
                    !releaseLocks.contains(name)) {
                throw new DuplicateLockRequestException(String.format(
                        "A lock on %s is held by transaction %d and isn't being released.",
                        name, transaction.getTransNum()));
            }

            for (ResourceName rn: releaseLocks) {
                if (getLockType(transaction, rn) == LockType.NL) {
                    throw new NoLockHeldException(String.format(
                            "No lock on a %s in RELEASELOCKS is held by transaction %d.",
                                name, transaction.getTransNum()));
                }
            }

            entry = getResourceEntry(name);
            lock = new Lock(name, lockType, txcNum);
            if (entry.compatibleWithOthers(transaction, lockType)) {
                granted = true;
                release(transaction, releaseLocks);
                entry.locks.add(0, lock);
                addLockToTransaction(txcNum, lock);
            } else {
                entry.waitingQueue.addFirst(new LockRequest(transaction,
                        lock, txcReleaseLocks(transaction, releaseLocks)));
                transaction.prepareBlock();
            }
        }
        if (!granted) transaction.block();
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        ResourceEntry entry; Lock lock;
        long txcNum = transaction.getTransNum();
        boolean granted = false;
        synchronized (this) {
            if (getLockType(transaction, name) != LockType.NL) {
                throw new DuplicateLockRequestException(String.format(
                        "A lock on %s is held by transaction %d.",
                        name, transaction.getTransNum()));
            }

            entry = getResourceEntry(name);
            lock = new Lock(name, lockType, txcNum);
            if (entry.compatible(lockType)) {
                granted = true;
                entry.locks.add(lock);
                addLockToTransaction(txcNum, lock);
            } else {
                entry.waitingQueue.addLast(
                        new LockRequest(transaction, lock));
                transaction.prepareBlock();
            }
        }
        if (!granted) transaction.block();
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        ResourceEntry entry;
        long txcNum = transaction.getTransNum();
        Lock lock; LockType lockType;
        synchronized (this) {
            if ((lockType = getLockType(transaction, name)) == LockType.NL) {
                throw new NoLockHeldException(String.format(
                        "A lock on %s is not held by transaction %d.",
                        name, transaction.getTransNum()));
            }

            entry = getResourceEntry(name);
            lock = new Lock(name, lockType, txcNum);
            removeLockFromTransaction(txcNum, lock);
            entry.release(lock);
        }
        transaction.unblock();
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        ResourceEntry entry;
        long txcNum = transaction.getTransNum();
        Lock newLock, oldLock; LockType lockType;
        boolean granted = false;
        synchronized (this) {
            if ((lockType = getLockType(transaction, name)) == newLockType) {
                throw new DuplicateLockRequestException(String.format(
                        "A %s type lock on %s is already held by transaction %d.",
                        newLockType, name, transaction.getTransNum()));
            } else if (lockType == LockType.NL) {
                throw new NoLockHeldException(String.format(
                        "A lock on %s is not held by transaction %d.",
                        name, transaction.getTransNum()));
            } else if (!LockType.substitutable(newLockType, lockType)) {
                throw new InvalidLockException(String.format(
                        "A promotion from lock type %s to lock type %s is invalid on %s made by transaction %d.",
                        lockType, newLockType, name, transaction.getTransNum()));
            }

            entry = getResourceEntry(name);
            if (entry.compatiblePromote(transaction, newLockType)) {
                oldLock = new Lock(name, lockType, txcNum);
                promoteTransactionLock(txcNum, oldLock, newLockType);
                entry.promote(transaction, newLockType);
                granted = true;
            } else {
                newLock = new Lock(name, newLockType, txcNum);
                entry.waitingQueue.addFirst(
                        new LockRequest(transaction, newLock));
                transaction.prepareBlock();
            }
        }
        if (!granted) transaction.block();
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(hw4_part1): implement
        for (Lock lock : getTransactionLocks(transaction.getTransNum())) {
            if (lock.name.equals(name))
                return lock.lockType;
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
