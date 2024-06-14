package com.seqera;

public class SerialDataProcessor implements DataProcessor {

  @Override
  public void run(DataSource dataSource, DataDestination dataDestination) {

    while(dataSource.hasNext()) {
      InputRecord inputRecord = dataSource.next();
      dataDestination.write(new OutputRecord(
          inputRecord.key() + inputRecord.value(),
          inputRecord.key() * inputRecord.value()
      ));
    }
  }

  @Override
  public void close() {
    // Do nothing
  }
}
