package com.vv.kstream.model;

import java.time.LocalTime;

public class TimeRange {
    final LocalTime start;
    final LocalTime end;

    public TimeRange(String start, String end) {
        this.start = LocalTime.parse(start);
        this.end = LocalTime.parse(end.equals("00:15") ? "23:59" : end); // workaround for midnight
    }

    public boolean contains(LocalTime time) {
        return !time.isBefore(start) && !time.isAfter(end);
    }

    @Override
    public String toString() {
        return "TimeRange{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}