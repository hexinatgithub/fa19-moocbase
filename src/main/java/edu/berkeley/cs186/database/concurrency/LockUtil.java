package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) return;
        LockType currentType = lockContext.getExplicitLockType(transaction),
                newLockType;

        if (lockContext.readonly || LockType.substitutable(currentType, lockType) ||
                lockType == LockType.NL)
            return;

        newLockType = combine(currentType, lockType);
        ensureSufficientAncestorsLockHeld(transaction, lockContext, newLockType);
        acquireLock(transaction, lockContext, newLockType);
    }

    // lockContext must be table's page.
    public static void ensureSufficientLockHeld(LockContext pageContext, LockType lockType,
                                                boolean autoEscalate) {
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null) return;
        LockContext tableContext = pageContext.parentContext();
        LockType temp = pageContext.getExplicitLockType(transaction);

        if (autoEscalate && temp == LockType.NL &&
                tableContext.saturation(transaction) >= 0.2 &&
                tableContext.capacity() >= 10) {
            tableContext.escalate(transaction);
            temp = tableContext.getEffectiveLockType(transaction);
            if (!LockType.substitutable(temp, lockType))
                ensureSufficientLockHeld(pageContext, lockType);
        } else
            ensureSufficientLockHeld(pageContext, lockType);
    }

    // TODO(hw4_part2): add helper methods as you see fit
    private static void ensureSufficientAncestorsLockHeld(TransactionContext transaction,
                                                          LockContext lockContext, LockType lockType) {
        LockContext parent = lockContext.parentContext();
        if (parent == null) return;
        LockType currentParentType = parent.getExplicitLockType(transaction),
                leastParentType, newParentType;
        if (LockType.canBeParentLock(currentParentType, lockType)) return;

        leastParentType = LockType.parentLock(lockType);
        newParentType = combine(currentParentType, leastParentType);
        ensureSufficientAncestorsLockHeld(transaction, parent, newParentType);
        acquireLock(transaction, parent, newParentType);
    }

    private static void acquireLock(TransactionContext transaction,
                                    LockContext lockContext, LockType lockType) {
        LockType currentType = lockContext.getExplicitLockType(transaction);
        LockContext parent = lockContext.parentContext();
        LockType parentEffectiveLockType = parent != null ?
                parent.getEffectiveLockType(transaction) : LockType.NL;

        if (LockType.substitutable(parentEffectiveLockType, lockType))
            return;

        if (currentType == LockType.NL)
            lockContext.acquire(transaction, lockType);
        else if ((currentType == LockType.IS && lockType == LockType.S) ||
                ((currentType == LockType.IX ||
                        currentType == LockType.SIX) && lockType == LockType.X))
            lockContext.escalate(transaction);
        else
            lockContext.promote(transaction, lockType);
    }

    /**
     * This method returns the lock that at least can do both lock can do.
     */
    private static LockType combine(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }

        if (a == LockType.X || b == LockType.X)
            return LockType.X;

        switch (a) {
            case S:
                switch (b) {
                    case IX:
                    case SIX:
                        return LockType.SIX;
                    case IS:
                    case S:
                    case NL:
                    default: return LockType.S;
                }
            case IX:
                switch (b) {
                    case S:
                    case SIX: return LockType.SIX;
                    case IS:
                    case NL:
                    case IX:
                    default: return LockType.IX;
                }
            case SIX: return LockType.SIX;
            case IS:
                switch (b) {
                    case IX: return LockType.IX;
                    case SIX: return LockType.SIX;
                    case S: return LockType.S;
                    case NL:
                    case IS:
                    default: return LockType.IS;
                }
            case NL:
            default: return b;
        }
    }
}
