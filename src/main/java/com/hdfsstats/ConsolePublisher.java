/*
 * Copyright © 2015 Tesla Motors, Inc. All rights reserved.
 */

package com.hdfsstats;

import java.io.IOException;
import java.util.List;

public class ConsolePublisher implements HdfsStatsPublisher {
  public void handleDependencies(final List<HdfsStats> stats) {
    // no-op
  }

  public void publish(final List<HdfsStats> stats) {
    for (HdfsStats stat : stats) {
      System.out.println(stat);
    }
  }

  public void close() throws IOException {
     // no-op
  }
}
