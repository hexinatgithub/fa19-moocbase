package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(hw4_part1): implement
        checkReadonly();
        checkRequestDuplicateLock(transaction);
        checkRequestConflictLock(transaction, lockType);
        checkRequestRedundantLock(transaction, lockType);
        lockman.acquire(transaction, name, lockType);
        updateParentNumChildLocks(transaction, +1);
    }

    private void updateParentNumChildLocks(TransactionContext transaction, int num) {
        LockContext parent;
        if ((parent = parentContext()) == null) return;
        parent.updateNumChildLocks(transaction, num);
    }

    private void updateNumChildLocks(TransactionContext transaction, int num) {
        long transNum = transaction.getTransNum();
        int numLocks = numChildLocks.getOrDefault(transNum, 0);
        numChildLocks.put(transNum, Math.max(numLocks + num, 0));
        if (numChildLocks.get(transNum) == 0)
            numChildLocks.remove(transNum);
        updateParentNumChildLocks(transaction, num);
    }

    private void checkReadonly() {
        if (readonly) {
            throw new UnsupportedOperationException(String.format(
                    "Readonly context: %s", this));
        }
    }

    private void checkRequestDuplicateLock(TransactionContext transaction)
            throws DuplicateLockRequestException {
        if (getExplicitLockType(transaction) != LockType.NL) {
            throw new DuplicateLockRequestException(String.format(
                    "A lock on %s is already hold by transaction %d.",
                    name, transaction.getTransNum()));
        }
    }

    private void checkRequestConflictLock(TransactionContext transaction, LockType lockType)
            throws InvalidLockException {
        LockContext parent = parentContext();
        if (parent == null) return;
        LockType temp = parent.getExplicitLockType(transaction);
        if (!LockType.canBeParentLock(temp, lockType)) {
            throw new InvalidLockException(String.format(
                    "Transaction %d request a conflict lock %s, parent explicit lock type is %s.",
                    transaction.getTransNum(), lockType, temp));
        }
    }

    private void checkRequestRedundantLock(TransactionContext transaction, LockType lockType)
            throws InvalidLockException {
        LockContext parent = parentContext();
        if (parent == null) return;
        LockType temp = parent.getEffectiveLockType(transaction);
        if (isRedundantLock(temp, lockType)) {
            throw new InvalidLockException(String.format(
                    "Transaction %d request a redundant lock %s, parent effective lock type is %s.",
                    transaction.getTransNum(), lockType, temp));
        }
    }

    private static boolean isRedundantLock(LockType ancestorEffectiveType, LockType lockType) {
        switch (ancestorEffectiveType) {
        case S:
            return lockType == LockType.S ||
                    lockType == LockType.IS;
        case X:
            return true;
        case NL:
        default: return false;
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(hw4_part1): implement
        checkReadonly();
        checkHeldLock(transaction);
        checkReleaseConstraints(transaction);
        lockman.release(transaction, name);
        updateParentNumChildLocks(transaction, -1);
    }

    // check held lock to release/promote/escalate
    private void checkHeldLock(TransactionContext transaction)
            throws NoLockHeldException{
        if (getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException(String.format(
                    "No lock on %s is hold by transaction %d.",
                    name, transaction.getTransNum()));
        }
    }

    // check multigranularity locking constraints
    private void checkReleaseConstraints(TransactionContext transaction)
            throws InvalidLockException{
        LockContext children;
        LockType childType;
        for (int i = 0; i < capacity(); i++) {
            children = childContext(i);
            childType = children.getExplicitLockType(transaction);
            if (childType != LockType.NL) {
                throw new InvalidLockException(String.format(
                        "Transaction %d can't released %s lock on context %s, child context %s hold a %s lock.",
                        transaction.getTransNum(), getExplicitLockType(transaction), name,
                        children.name, childType));
            }
        }
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(hw4_part1): implement
        checkReadonly();
        checkHeldLock(transaction);
        upgrade(transaction, newLockType);
    }

    // upgrade lock to NEWLOCKTYPE, release any descendant redundant lock is needed.
    private void upgrade(TransactionContext transaction, LockType lockType) {
        upgrade(transaction, lockType, false);
    }

    // upgrade lock to NEWLOCKTYPE, release any descendant redundant lock is needed.
    private void upgrade(TransactionContext transaction, LockType lockType, boolean escalate)
            throws InvalidLockException {
        checkUpgradeLock(transaction, lockType);
        checkUpgradeConstraints(transaction, lockType);
        long transNum = transaction.getTransNum();
        int childLocksNum = numChildLocks.getOrDefault(transNum, 0);
        boolean clearDescendantLocks = escalate ||
                ((lockType == LockType.X || lockType == LockType.S) && childLocksNum > 0);
        List<ResourceName> releases = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        LockType type;

        if (clearDescendantLocks) {
            for (Lock lock : locks) {
                if ((lock.name.isDescendantOf(name)) ||
                        lock.name.equals(name)) {
                    releases.add(lock.name);
                }
            }
        } else if (lockType == LockType.SIX) {
            type = effectiveLockType(lockType);
            for (Lock lock : locks) {
                if ((lock.name.isDescendantOf(name) &&
                        isRedundantLock(type, lock.lockType)) ||
                        lock.name.equals(name)) {
                    releases.add(lock.name);
                }
            }
        }

        if (releases.size() > 0) {
            lockman.acquireAndRelease(transaction, name, lockType, releases);
            updateNumChildLocks(transaction, -releases.size() + 1);
        } else
            lockman.promote(transaction, name, lockType);
    }

    private void checkUpgradeLock(TransactionContext transaction, LockType newLockType)
            throws InvalidLockException {
        LockType lockType = getExplicitLockType(transaction);
        if (lockType == newLockType) {
            throw new DuplicateLockRequestException(String.format(
                    "A %s lock on %s is already hold by transaction %d.",
                    lockType, name, transaction.getTransNum()));
        }

        if (!LockType.substitutable(newLockType, lockType)) {
            throw new InvalidLockException(String.format(
                    "Transaction %d can't substitutable lock %s with %s on context %s.",
                    transaction.getTransNum(), lockType, newLockType, name));
        }
    }

    // check promote would cause invalid state.
    private void checkUpgradeConstraints(TransactionContext transaction, LockType lockType)
            throws InvalidLockException {
        LockContext parent = parentContext();
        if (parent == null) return;
        LockType temp = parent.getEffectiveLockType(transaction);
        if (LockType.canBeParentLock(temp, lockType)) {
            throw new InvalidLockException(String.format(
                    "Transaction %d can't promote lock from %s to %s on context %s, parent context %s hold a %s lock.",
                    transaction.getTransNum(), getExplicitLockType(transaction), lockType, name,
                    parent.name, temp));
        }
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(hw4_part1): implement
        checkReadonly();
        checkHeldLock(transaction);
        switch (getExplicitLockType(transaction)) {
        case IS:
            upgrade(transaction, LockType.S, true);
            break;
        case SIX:
        case IX:
            upgrade(transaction, LockType.X, true);
            break;
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(hw4_part1): implement
        LockContext lockContext = this;
        LockType temp;
        while (lockContext != null) {
            temp = lockman.getLockType(transaction, lockContext.name);
            temp = effectiveLockType(temp);
            if (temp != LockType.NL) return temp;
            lockContext = lockContext.parentContext();
        }
        return LockType.NL;
    }

    private static LockType effectiveLockType(LockType lockType) {
        switch (lockType) {
            case SIX:
            case S: return LockType.S;
            case X: return LockType.X;
        }
        return LockType.NL;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(hw4_part1): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

