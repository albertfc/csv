package com.seqera;

import static com.seqera.Utils.createOutputFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class CSVDataDestinationTest {

  @Test
  void testWrite() throws IOException {
    List<OutputRecord> expected = List.of(new OutputRecord(1., 2.), new OutputRecord(3., 4.));
    File outputFile = createOutputFile();

    CSVDataDestination csvDataDestination = new CSVDataDestination(outputFile);
    expected.forEach(csvDataDestination::write);
    csvDataDestination.close();

    List<OutputRecord> actual = Utils.readOutputCsvFile(outputFile);
    assertEquals(expected, actual);
  }

}