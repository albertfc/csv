package com.seqera;

import java.util.concurrent.Future;

public record ResultMessage(Future<Double> adderFuture, Future<Double> multiplierFuture) implements
    Message {

  @Override
  public MessageType getType() {
    return MessageType.RESULT;
  }

  @Override
  public ResultMessage asResultMessage() {
    return this;
  }
}
