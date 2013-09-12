package org.apache.drill.exec.engine.async;

public interface LeafDriver extends Runnable {
  void stop();
}
