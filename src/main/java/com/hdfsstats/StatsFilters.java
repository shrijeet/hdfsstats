package com.hdfsstats;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * Some common <code>StatsFilter</code> implementations.
 */
public enum StatsFilters implements HdfsStatsBuilder.StatsFilter {

  /**
   * A <code>StatsFilter</code> implementations which rejects <code>HdfsStats</code> with too few
   * files & directories & small consumed space. This is useful to cut-short the long tail when
   * dealing with Namenode serving huge number of directories.
   *
   * Filter also rejects folder with YYYYMMDD type of pattern because they are most likely per day
   * folder and once persisted size etc. is most likely not going to change. So for those cases
   * it makes no sense to track them.
   */
  SIGNIFICANCE_BASED_FILTER {
    private static final long DIR_COUNT_THRESHOLD = 10;
    private static final long FILE_COUNT_THRESHOLD = 1000;
    private static final long SPACE_THRESHOLD = 107374182400L; //100GB
    private final Pattern YYYYMMDD =
        Pattern.compile(".*(19|20)\\d\\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01]).*");

    @Override
    public boolean accept(HdfsStats hdfsStats) {
      boolean hasSignificantDirs = hdfsStats.getDirCount() >= DIR_COUNT_THRESHOLD;
      boolean hasSignificantFiles = hdfsStats.getFileCount() >= FILE_COUNT_THRESHOLD;
      boolean consumesSignificantSpace = hdfsStats.getSpaceConsumed() >= SPACE_THRESHOLD;
      Matcher m = YYYYMMDD.matcher(hdfsStats.getPath());
      return (hasSignificantDirs || hasSignificantFiles || consumesSignificantSpace)
          && !m.matches();
    }

  },

  /**
   * Depth based filter accepts all paths up-to fixed depth. For example if fixed depth is 2 it will
   * accept /user/hdfs & /user but it will not accept /user/hdfs/warehouse. It can be useful to
   * record hdfs stats which have been otherwise identified as insignificant or with low ROI but if
   * they are 'top level' i.e too deep. For example /tmp & path immediately under /tmp will never
   * get ignored if this filter accepts them.
   */
  DEPTH_BASED_FILTER {
    private static final long DEPTH = 2;
    private final Splitter splitter = Splitter.on('/');

    @Override
    public boolean accept(HdfsStats hdfsStats) {
      String path = hdfsStats.getPath();
      int count = Iterables.size(splitter.split(path)) - 1;
      return count <= DEPTH;
    }
  };

  public boolean accept(HdfsStats hdfsStats) {
    return true;
  }

}