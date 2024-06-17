package com.seqera;

import static com.seqera.Utils.createInputFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class CSVDataSourceTest {

  @Test
  void testRead() throws IOException {
    List<InputRecord> expected = List.of(new InputRecord(1, 2.), new InputRecord(3, 4.));
    File inputFile = createInputFile(expected);

    CSVDataSource csvDataSource = new CSVDataSource(inputFile);
    List<InputRecord> actual = StreamSupport.stream(
        ((Iterable<InputRecord>) () -> csvDataSource).spliterator(), false).toList();
    csvDataSource.close();

    assertEquals(expected, actual);
  }

}