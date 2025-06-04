package com.vv.kstream.model;

public record VisitStatus(String location, String time, String status) {
    @Override
    public String toString() {
        return location + "," + time + "," + status;
    }
}
