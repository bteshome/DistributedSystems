package com.bteshome.keyvaluestore.common;

import java.io.Serializable;
import java.util.Objects;

public record LogPosition(int leaderTerm, long index) implements Serializable {
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

    public boolean equals(LogPosition other) {
        return compare(this, other) == 0;
    }

    public boolean equals(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) == 0;
    }

    public boolean isGreaterThan(LogPosition other) {
        return compare(this, other) > 0;
    }

    public boolean isGreaterThan(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) > 0;
    }

    public boolean isGreaterThanOrEquals(LogPosition other) {
        return compare(this, other) >= 0;
    }

    public boolean isGreaterThanOrEquals(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) >= 0;
    }

    public boolean isLessThan(LogPosition other) {
        return compare(this, other) < 0;
    }

    public boolean isLessThan(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) < 0;
    }

    public boolean isLessThanOrEquals(LogPosition other) {
        return compare(this, other) <= 0;
    }

    public boolean isLessThanOrEquals(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) <= 0;
    }

    private static int compare(LogPosition position, int leaderTerm, long index) {
        return compare(position.leaderTerm(), position.index(), leaderTerm, index);
    }

    private static int compare(LogPosition position1, LogPosition position2) {
        return compare(position1.leaderTerm(), position1.index(), position2.leaderTerm(), position2.index());
    }

    public static int compare(int leaderTerm1, long index1, int leaderTerm2, long index2) {
        if (leaderTerm1 == leaderTerm2)
            return Long.compare(index1, index2);
        return Integer.compare(leaderTerm1, leaderTerm2);
    }
}
