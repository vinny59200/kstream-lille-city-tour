package com.vv.kstream.util;

import com.vv.kstream.model.TimeRange;
import com.vv.kstream.model.VisitStatus;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;

public class VisitStatusUtils {

    // Static map of valid time ranges per location
    public static final Map<String, List<TimeRange>> VALID_TIMETABLES = Map.of(
            "Gare Lille Flandres", List.of(new TimeRange("04:35", "00:15")),
            "St Maurice church", List.of(new TimeRange("11:00", "18:00")),
            "Restaurant Les Moules", List.of(new TimeRange("12:00", "15:00"), new TimeRange("19:00", "22:00")),
            "Place Charles de Gaulle", List.of(new TimeRange("00:00", "23:59")),
            "Vieille Bourse", List.of(new TimeRange("13:00", "19:00")),
            "Opera", List.of(new TimeRange("01:30", "18:00")),
            "Beffroi", List.of(new TimeRange("10:00", "13:00"), new TimeRange("14:00", "17:30")),
            "Place Louise de Bettignies", List.of(new TimeRange("00:00", "23:59")),
            "Cathedral Notre Dame de la Treille", List.of(new TimeRange("10:30", "18:15"))
                                                                              );

    /**
     * Parses and validates a CSV visit event.
     */
    public static VisitStatus processVisit(String csvLine) {
        try {
            String[] parts = csvLine.split(",");
            String location = parts[0].trim();
            String time = parts[1].trim();
            LocalTime visitTime = LocalTime.parse(time);

            boolean isValid = VALID_TIMETABLES.getOrDefault(location, List.of()).stream()
                                              .anyMatch(range -> range.contains(visitTime));

            return new VisitStatus(location, time, isValid ? "OK" : "KO");
        } catch (Exception e) {
            return new VisitStatus("Invalid", "Invalid", "KO");
        }
    }

}
