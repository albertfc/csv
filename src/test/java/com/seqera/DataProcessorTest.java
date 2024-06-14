package com.seqera;

import static com.seqera.Utils.createInputFile;
import static com.seqera.Utils.createOutputFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class DataProcessorTest {

  @ParameterizedTest
  @MethodSource
  void testRun(DataProcessor dataProcessor) throws IOException {
    List<InputRecord> inputRecords     = List.of(new InputRecord(1, 2.), new InputRecord(3,  4.));
    List<OutputRecord> expectedRecords = List.of(new OutputRecord(3., 2.), new OutputRecord(7., 12.));

    File inputFile = createInputFile(inputRecords);
    DataSource dataSource = new CSVDataSource(inputFile);
    File outputFile = createOutputFile();
    DataDestination dataDestination = new CSVDataDestination(outputFile);
    dataProcessor.run(dataSource, dataDestination);
    dataDestination.close();

    List<OutputRecord> actualRecords = Utils.readOutputCsvFile(outputFile);
    assertEquals(expectedRecords, actualRecords);
  }

  private static Stream<DataProcessor> testRun() {
    return Stream.of(
        new SerialDataProcessor(),
        new ParallelDataProcessor());
  }
}