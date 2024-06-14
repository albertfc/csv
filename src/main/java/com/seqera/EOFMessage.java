package com.seqera;

public class EOFMessage implements Message {

  @Override
  public MessageType getType() {
    return MessageType.EOF;
  }
}
