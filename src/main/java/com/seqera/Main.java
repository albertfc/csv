package com.seqera;

import java.io.File;
import java.util.logging.Logger;

public class Main {

  public static final String INPUT_CSV = "input.csv";
  public static final String OUTPUT_CSV = "output.csv";

  public static void main(String[] args) {

    Logger logger = Logger.getLogger(Main.class.getName());

    File inputFile = new File(INPUT_CSV);
    File outputFile = new File(OUTPUT_CSV);
    logger.info("Input file: " + inputFile.getAbsolutePath());
    logger.info("Output file: " + outputFile.getAbsolutePath());
    logger.info("Processing...");

    try (
        DataSource dataSource = new CSVDataSource(inputFile);
        DataDestination dataDestination = new CSVDataDestination(outputFile);
        DataProcessor dataProcessor = new ParallelDataProcessor()
    ) {
      dataProcessor.run(dataSource, dataDestination);
    } catch (Exception e) {
      throw new SeqeraException("Error processing: " + e.getMessage(), e);
    }
    logger.info("End of processing");
  }
}