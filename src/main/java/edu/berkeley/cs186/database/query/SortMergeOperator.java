package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(hw3_part1): implement
            SortOperator leftSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(),
                    this.getLeftTableName(), new LeftRecordComparator());
            SortOperator rightSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(),
                    this.getRightTableName(), new RightRecordComparator());
            this.leftIterator = SortMergeOperator.this.getRecordIterator(leftSortOperator.sort());
            this.rightIterator = SortMergeOperator.this.getRecordIterator(rightSortOperator.sort());

            this.nextRecord = null;
            this.marked = false;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(hw3_part1): implement
            if (this.leftRecord == null || this.rightRecord == null) {
                throw new NoSuchElementException("No new record to fetch");
            }
            this.nextRecord = null;
            do {
                if (marked) {   // fetch next record in current partition
                    /*
                      leftRecord and rightRecord must not null and
                      leftRecord must equal to rightRecord due to findPartition,
                     */
                    assert leftRecord != null && rightRecord != null;
                    this.nextRecord = joinRecords(leftRecord, rightRecord);
                    nextRightRecord();
                    if (rightRecord == null ||
                            compareLeftRightRecord() < 0) {
                        nextLeftRecord();
                        resetRightIterator();
                        if (leftRecord != null && compareLeftRightRecord() > 0)
                            marked = false;
                    }
                } else {
                    findPartition();
                }
            } while (!hasNext());
        }

        private void findPartition() {
            while (compareLeftRightRecord() < 0) {
                nextLeftRecord();
                if (leftRecord == null) {
                    throw new NoSuchElementException("All Done!");
                }
            }

            while (compareLeftRightRecord() > 0) {
                nextRightRecord();
                if (rightRecord == null) {
                    throw new NoSuchElementException("All Done!");
                }
            }

            rightIterator.markPrev();
            marked = true;
        }

        private void resetRightIterator() {
            rightIterator.reset();
            rightRecord = rightIterator.next();
        }

        /**
         * Advances the left record
         *
         * The thrown exception means we're done: there is no next record
         * It causes this.fetchNextRecord (the caller) to hand control to its caller.
         */
        private void nextLeftRecord() {
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
        }

        private void nextRightRecord() {
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
        }

        private int compareLeftRightRecord() {
            DataBox value1 = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
            DataBox value2 = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
            return value1.compareTo(value2);
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(hw3_part1): implement
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(hw3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
