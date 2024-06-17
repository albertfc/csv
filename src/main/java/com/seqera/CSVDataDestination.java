package com.seqera;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.MessageFormat;

public class CSVDataDestination implements DataDestination {

  private final PrintWriter printWriter;

  public CSVDataDestination(File outputFile) {
    try {
      printWriter = new PrintWriter(outputFile);
    } catch (FileNotFoundException e) {
      throw new SeqeraException("Error accessing CSV output file", e);
    }
    printWriter.println("#sum, #prod");
  }

  @Override
  public void write(OutputRecord outputRecord) {
    printWriter.println(MessageFormat.format(
        "{0,number,#.########}, {1,number,#.########}",
        outputRecord.sum(), outputRecord.prod()));
  }

  @Override
  public void close() {
    printWriter.close();
  }
}
