package com.hdfsstats;

import java.io.Closeable;
import java.util.List;


/**
 * Build the hdfs stats
 */
public interface HdfsStatsBuilder extends Closeable {

  List<HdfsStats> build();

  /**
   * <cod>StatsFilter</cod> can be used by <code>HdfsStatsBuilder</code> implementations to restrict
   * the number of <code>HdfsStats</code> instances they return.
   */
  interface StatsFilter {
    /**
     * @param hdfsStats an hdfs stats instance
     * @return true if the instances passes filtering criteria
     */
    boolean accept(HdfsStats hdfsStats);
  }

}
