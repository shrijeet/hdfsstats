package com.hdfsstats;

import java.io.Closeable;
import java.util.List;

/**
 * Publish hdfs stats to data store
 */
public interface HdfsStatsPublisher extends Closeable {

  /**
   * Create dependencies for the hdfs stats before publishing them. This could be useful to create
   * meta data associated with the stats. For example if data table references other tables, data in
   * meta tables can be inserted prior to publishing via this method call.
   *
   * @param stats the stats for which dependencies have to be handle
   */
  public void handleDependencies(List<HdfsStats> stats);

  /**
   * Record the statistics in Database
   *
   * @param stats the stats to publish
   */
  public void publish(List<HdfsStats> stats);

}
