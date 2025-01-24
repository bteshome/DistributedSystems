package com.bteshome.keyvaluestore.common;

import java.util.Objects;

public record LogPosition(int leaderTerm, long index) implements Comparable<LogPosition> {
    private static final LogPosition empty = new LogPosition(0, 0);

    public static LogPosition empty() {
        return empty;
    }

    public static LogPosition of(int leaderTerm, long index) {
        return new LogPosition(leaderTerm, index);
    }

    @Override
    public String toString() {
        return String.format("<term=%d,index=%d>", leaderTerm, index);
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
        if (leaderTerm == o.leaderTerm)
            return Long.compare(index, o.index);
        return Integer.compare(leaderTerm, o.leaderTerm);
    }
}
