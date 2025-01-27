package com.bteshome.keyvaluestore.common;

import java.io.Serializable;
import java.util.Objects;

public record LogPosition(int leaderTerm, long index) implements Serializable, Comparable<LogPosition> {
    private static final LogPosition empty = new LogPosition(0, 0);

    public static LogPosition empty() {
        return empty;
    }

    public static LogPosition of(int leaderTerm, long index) {
        return new LogPosition(leaderTerm, index);
    }

    @Override
    public String toString() {
        return String.format("%d:%d", leaderTerm, index);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LogPosition that = (LogPosition) o;
        return index == that.index && leaderTerm == that.leaderTerm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderTerm, index);
    }

    @Override
    public int compareTo(LogPosition o) {
        return compare(this, o.leaderTerm(), o.index());
    }

    public static int compare(LogPosition position, int leaderTerm, long index) {
        return compare(position.leaderTerm(), position.index(), leaderTerm, index);
    }

    public static int compare(int leaderTerm1, long index1, int leaderTerm2, long index2) {
        if (leaderTerm1 == leaderTerm2)
            return Long.compare(index1, index2);
        return Integer.compare(leaderTerm1, leaderTerm2);
    }
}
