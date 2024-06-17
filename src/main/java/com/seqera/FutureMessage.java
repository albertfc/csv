package com.seqera;

import java.util.concurrent.Future;

public record FutureMessage(Future<Double> adderFuture, Future<Double> multiplierFuture) implements
    Message {

  @Override
  public MessageType getType() {
    return MessageType.FUTURE;
  }

  @Override
  public FutureMessage asFutureMessage() {
    return this;
  }
}
