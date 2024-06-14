package com.seqera;

public interface Message {
  MessageType getType();

  default ResultMessage asResultMessage() {
    throw new UnsupportedOperationException();
  }
}
