package com.seqera;

public interface Message {
  MessageType getType();

  default FutureMessage asFutureMessage() {
    throw new UnsupportedOperationException();
  }
}
