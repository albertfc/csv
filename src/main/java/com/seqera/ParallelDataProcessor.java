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
    // A class to collect results and write them to the destination

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
        // This loop reads messages send by the main thread and writes the results to the destination
        // All operations are blocking
        Message message = blockingQueue.take();
        while (!message.getType().equals(MessageType.EOF)) {
          // Wait for the results of the adder and multiplier tasks to become available
          FutureMessage futureMessage = message.asFutureMessage();
          Double adderResult = futureMessage.adderFuture().get();
          Double multiplierResult = futureMessage.multiplierFuture().get();
          // Write the results to the destination
          dataDestination.write(new OutputRecord(adderResult, multiplierResult));
          // Get the next message, waiting if not available
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
    // Queue to communicate results between main and collector thread using a producer-consumer pattern
    BlockingQueue<Message> blockingQueue = new LinkedBlockingQueue<>();
    Future<?> collector = executor.submit(new Collector(blockingQueue, dataDestination));

    try {
      // This loop reads input records from the data source and submits tasks to the executor
      // to be executed in parallel. The results are then gathered by the collector to write to the destination.
      // The loop runs without blocking except when the blockingQueue is full and/or the data source
      // is blocked by I/O operations.
      while(dataSource.hasNext()) {
        InputRecord inputRecord = dataSource.next();
        // Submit tasks to the executor to execute in parallel
        Future<Double> adderFuture = executor.submit(new Adder(inputRecord));
        Future<Double> multiplierFuture = executor.submit(new Multiplier(inputRecord));
        // Send the futures to the collector to retrieve the results when they are ready
        blockingQueue.put(new FutureMessage(adderFuture, multiplierFuture));
      }
      blockingQueue.put(new EOFMessage());  // signal the end of processing
      collector.get();  // wait for collector to finish
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SeqeraException("Main thread has been interrupted", e);
    } catch (ExecutionException e) {
      throw new SeqeraException("Error executing main thread", e);
    }
  }
}
