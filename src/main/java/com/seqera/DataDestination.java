package com.seqera;

import java.io.Closeable;

public interface DataDestination extends Closeable {

  void write(OutputRecord outputRecord);
}
