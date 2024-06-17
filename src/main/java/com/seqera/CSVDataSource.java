package com.seqera;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;

public class CSVDataSource implements DataSource {

  public static final String COMMA_DELIMITER = ",";
  private final Iterator<InputRecord> records;
  private final BufferedReader reader;

  public CSVDataSource(File inputFile) {
    try {
      // Build a buffered iterator over the input records, avoiding reading the whole file into memory
      reader = Files.newBufferedReader(inputFile.toPath());
      records = reader.lines()
          .filter(header -> !header.startsWith("#"))
          .map(line -> line.split(COMMA_DELIMITER))
          .filter(input -> input.length == 2)
          .map(input -> new InputRecord(
              Integer.parseInt(input[0].trim()),
              Double.parseDouble(input[1].trim())))
          .iterator();
    } catch (IOException e) {
      throw new SeqeraException("Error accessing CSV input file", e);
    }
  }

  @Override
  public boolean hasNext() {
    return records.hasNext();
  }

  @Override
  public InputRecord next() {
    return records.next();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
