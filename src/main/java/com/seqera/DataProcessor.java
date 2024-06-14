package com.seqera;

import java.io.Closeable;

public interface DataProcessor extends Closeable {
  void run(DataSource dataSource, DataDestination dataDestination);
}
