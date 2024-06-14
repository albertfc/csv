package com.seqera;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class ParallelDataProcessor implements DataProcessor {

  private final ExecutorService executor;

  public ParallelDataProcessor() {
    int nThreads = Runtime.getRuntime().availableProcessors();
    executor = Executors.newFixedThreadPool(nThreads);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }

  static class Adder implements Callable<Double> {

    private final InputRecord inputRecord;

    public Adder(InputRecord inputRecord) {
      this.inputRecord = inputRecord;
    }

    @Override
    public Double call() {
      return inputRecord.key() + inputRecord.value();
    }
  }

  static class Multiplier implements Callable<Double> {

    private final InputRecord inputRecord;

    public Multiplier(InputRecord inputRecord) {
      this.inputRecord = inputRecord;
    }

    @Override
    public Double call() {
      return inputRecord.key() * inputRecord.value();
    }
  }

  static class Collector implements Runnable {

    private final BlockingQueue<Message> blockingQueue;
    private final DataDestination dataDestination;

    public Collector(BlockingQueue<Message> blockingQueue,
        DataDestination dataDestination) {
      this.blockingQueue = blockingQueue;
      this.dataDestination = dataDestination;
    }

    @Override
    public void run() {
      try {
        Message message = blockingQueue.take();
        while (!message.getType().equals(MessageType.EOF)) {
          ResultMessage resultMessage = message.asResultMessage();
          Double adderResult = resultMessage.adderFuture().get();
          Double multiplierResult = resultMessage.multiplierFuture().get();
          dataDestination.write(new OutputRecord(adderResult, multiplierResult));
          message = blockingQueue.take();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SeqeraException("Collector thread has been interrupted", e);
      } catch (ExecutionException e) {
        throw new SeqeraException("Error executing collector thread", e);
      }
    }
  }

  @Override
  public void run(DataSource dataSource, DataDestination dataDestination) {
    BlockingQueue<Message> blockingQueue = new LinkedBlockingQueue<>();
    Future<?> collector = executor.submit(new Collector(blockingQueue, dataDestination));

    try {
      while(dataSource.hasNext()) {
        InputRecord inputRecord = dataSource.next();
        Future<Double> adderFuture = executor.submit(new Adder(inputRecord));
        Future<Double> multiplierFuture = executor.submit(new Multiplier(inputRecord));
        blockingQueue.put(new ResultMessage(adderFuture, multiplierFuture));
      }
      blockingQueue.put(new EOFMessage());
      collector.get();  // wait for collector to finish
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SeqeraException("Main thread has been interrupted", e);
    } catch (ExecutionException e) {
      throw new SeqeraException("Error executing main thread", e);
    }
  }
}
