package com.hdfsstats;

import java.util.List;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import org.joda.time.DateTimeZone;

/**
 * Helper class for creating dummy hdfs stats
 */
public class RandomHdfsStats {

  public static final long DUMMY_RUNID = Long.MAX_VALUE;
  public static final long DUMMY_SPACE = 104857600L;
  public static final long DUMMY_SIZE = 104857600L;
  public static final long DUMMY_FILE_COUNT = 10;
  public static final long DUMMY_DIR_COUNT = 10;
  public static final long DUMMY_ACCESS_COUNT = 10;
  public static final String DUMMY_OWNER = "jondoe";
  public static final String DUMMY_PATH = "/path";

  /**
   * Create dummy stats for given cluster and path
   *
   * @param cluster cluster path belongs to
   * @param path    path to associate stats with
   * @return an HdfsStats instant
   */
  public HdfsStats createOne(String cluster, String path) {
    return createOne(cluster, path, DUMMY_OWNER, DUMMY_RUNID);
  }

  /**
   * Create bunch of hdfs stat instances for given cluster
   *
   * @param cluster cluster the paths would belong to
   * @param n       number of stat instances to create
   * @return bunch of <code>HdfsStats</code> instances
   */
  public List<HdfsStats> createLots(String cluster, int n) {
    List<HdfsStats> stats = Lists.newArrayList();
    for (int i = 0; i < n; i++) {
      stats.add(createOne(cluster, DUMMY_PATH + i));
    }
    return stats;
  }

  /**
   * Create dummy stats for given cluster and path
   *
   * @param cluster cluster path belongs to
   * @param path    path to associate stats with
   * @param owner   owner of the path
   * @return an HdfsStats instant
   */
  public HdfsStats createOne(String cluster, String path, String owner, long runId) {
    DateTime loadTime = utcDateTime(runId);
    return new HdfsStats(
        cluster,
        path,
        owner,
        loadTime,
        DUMMY_FILE_COUNT,
        DUMMY_DIR_COUNT,
        DUMMY_SPACE,
        DUMMY_SIZE,
        DUMMY_ACCESS_COUNT
    );
  }

  private static DateTime utcDateTime(long instant) {
    return new DateTime(instant, DateTimeZone.UTC);
  }

}