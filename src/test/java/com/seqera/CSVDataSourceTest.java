package com.seqera;

import static com.seqera.Utils.createInputFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class CSVDataSourceTest {

  @Test
  void testRead() throws IOException {
    List<InputRecord> expected = List.of(new InputRecord(1, 2.), new InputRecord(3, 4.));
    File inputFile = createInputFile(expected);

    CSVDataSource csvDataSource = new CSVDataSource(inputFile);
    int actualSize = 0;
    while (csvDataSource.hasNext()) {
      InputRecord inputRecord = csvDataSource.next();
      assertEquals(expected.get(actualSize), inputRecord);
      actualSize++;
    }
    csvDataSource.close();

    assertEquals(expected.size(), actualSize);
  }

}