package com.vv.kstream.util;

import com.vv.kstream.model.VisitStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VisitStatusUtilsTest {

    @Test
    void processVisit_validTime_shouldReturnOK() {
        // Given a valid location and time within valid range
        String csvLine = "Place Louise de Bettignies,22:07";

        // When processed
        VisitStatus result = VisitStatusUtils.processVisit(csvLine);

        // Then status should be OK
        assertEquals("OK", result.status());
        assertEquals("Place Louise de Bettignies", result.location());
        assertEquals("22:07", result.time());
    }

    @Test
    void processVisit_invalidTime_shouldReturnKO() {
        // Given a valid location but time outside valid range
        String csvLine = "Beffroi,13:30"; // Beffroi valid ranges are 10:00-13:00 and 14:00-17:30

        // When processed
        VisitStatus result = VisitStatusUtils.processVisit(csvLine);

        // Then status should be KO
        assertEquals("KO", result.status());
        assertEquals("Beffroi", result.location());
        assertEquals("13:30", result.time());
    }

    @Test
    void processVisit_unknownLocation_shouldReturnKO() {
        // Given unknown location
        String csvLine = "Unknown Place,12:00";

        // When processed
        VisitStatus result = VisitStatusUtils.processVisit(csvLine);

        // Then KO because no timetable for this location
        assertEquals("KO", result.status());
        assertEquals("Unknown Place", result.location());
        assertEquals("12:00", result.time());
    }

    @Test
    void processVisit_invalidCsvFormat_shouldReturnKO() {
        // Given malformed CSV input (missing comma)
        String csvLine = "InvalidInputWithoutComma";

        // When processed
        VisitStatus result = VisitStatusUtils.processVisit(csvLine);

        // Then status should be KO with "Invalid" location and time
        assertEquals("KO", result.status());
        assertEquals("Invalid", result.location());
        assertEquals("Invalid", result.time());
    }

    @Test
    void processVisit_invalidTimeFormat_shouldReturnKO() {
        // Given invalid time format
        String csvLine = "Opera,25:00";

        // When processed
        VisitStatus result = VisitStatusUtils.processVisit(csvLine);

        // Then KO with "Invalid" location and time because parsing LocalTime fails
        assertEquals("KO", result.status());
        assertEquals("Invalid", result.location());
        assertEquals("Invalid", result.time());
    }
}
