package com.seqera;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.List;

public class Utils {

  public static File createInputFile(List<InputRecord> inputRecords) throws IOException {
    File result = File.createTempFile("seqera-input-test", "csv");
    result.deleteOnExit();
    PrintWriter printWriter = new PrintWriter(result);
    printWriter.println("#key, #value");
    inputRecords.forEach(inputRecord ->
        printWriter.println(MessageFormat.format("{0}, {1}", inputRecord.key(), inputRecord.value())));
    printWriter.close();
    return result;
  }

  public static File createOutputFile() throws IOException {
    File result = File.createTempFile("seqera-output-test", "csv");
    result.deleteOnExit();
    return result;
  }

  public static List<OutputRecord> readOutputCsvFile(File csvFile) {
    try (BufferedReader reader = Files.newBufferedReader(csvFile.toPath())) {
      return reader.lines()
          .filter(header -> !header.startsWith("#"))
          .map(line -> line.split(","))
          .filter(input -> input.length == 2)
          .map(input -> new OutputRecord(
              Double.parseDouble(input[0].trim()),
              Double.parseDouble(input[1].trim())))
          .toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
