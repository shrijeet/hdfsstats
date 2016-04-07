package com.hdfsstats;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * <code>HdfsStatsBuilder</code> implementation that issues RPCs via FileSystem APIs & fetches
 * <code>ContentSummary</code> for all the directories starting from root directory.
 */
public class RPCBasedHdfsStatsBuilder implements HdfsStatsBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(RPCBasedHdfsStatsBuilder.class);
  private final String cluster;
  private final FileSystem fs;
  private final long now = System.currentTimeMillis() / 1000;
  private DateTime loadTime;
  private long contentSummaryCalls;
  private long listStatusCalls;
  private long skippedDirectories;
  private float SPACE_TO_DIR_RATIO = 100.0f;

  public RPCBasedHdfsStatsBuilder(String namenode, String cluster,
                                  DateTime loadTime) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namenode),
        "namenode param can't be empty/null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cluster),
        "cluster param can't be empty/null");
    Configuration conf = new Configuration();
    this.cluster = cluster;
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, namenode);
    this.fs = FileSystem.get(conf);
    this.loadTime = loadTime;
  }

  @VisibleForTesting
  RPCBasedHdfsStatsBuilder(FileSystem fs, String cluster, DateTime loadTime) {
    Preconditions.checkNotNull(fs);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(cluster));
    this.fs = fs;
    this.cluster = cluster;
    this.loadTime = loadTime;
  }

  public List<HdfsStats> build() {
    List<HdfsStats> allHdfsStats = Lists.newArrayList();
    try {
      FileStatus status = fs.getFileStatus(new Path("/"));
      internalBuild(status, allHdfsStats);

      LOG.info("Finished building, took " + (System.currentTimeMillis()/1000 - now) + " seconds.");
      LOG.info(String.format("Stats: content_summary_calls=%d, list_status_calls=%d, " +
          "skipped_directories=%d",contentSummaryCalls, listStatusCalls, skippedDirectories));

    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return allHdfsStats;
  }

  private void internalBuild(FileStatus status, List<HdfsStats> allHdfsStats)
      throws IOException {
    try {
      contentSummaryCalls++;
      ContentSummary summary = fs.getContentSummary(status.getPath());
      HdfsStats hdfsStats =createStats(cluster, status.getPath().toUri().getPath(),
          status.getOwner(), summary);
      boolean isSignificant = StatsFilters.SIGNIFICANCE_BASED_FILTER.accept(hdfsStats);
      boolean isTopLevelDir = StatsFilters.DEPTH_BASED_FILTER.accept(hdfsStats);
      if (isSignificant) {
        allHdfsStats.add(hdfsStats);
      }
      if (hdfsStats.getDirCount() == 0 || (!isSignificant && !isTopLevelDir)) {
        LOG.debug("Skipping {} because it was found to be insignificant.",
            hdfsStats.getPath());
        skippedDirectories++;
        return;
      }
      listStatusCalls++;
      FileStatus[] filesAndDirs = fs.listStatus(status.getPath());
      if (!isTopLevelDir && hasLowROI(filesAndDirs, hdfsStats)) {
        skippedDirectories++;
        return;
      }
      for (FileStatus subDir : filesAndDirs) {
        if (subDir.isDir()) {
          internalBuild(subDir, allHdfsStats);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.warn("{} was not found, ignoring & moving on.", status.getPath());
    }
  }

  /**
   * This method returns true if we think the return of 'investment' is going to be less. The
   * investment here is the cost of exploring a directory in pursuit of subdirectories that are
   * worth recording in hdfs stats index. The return of investment is judged on the basis of number
   * of subdirectories & total space occupied by the concerned path. For example if total space
   * occupied by a directory is 100GB but it has 1000 subdirectories. Per sub directory occupied
   * space footprint is quite small & it may not be worth publishing stats of individual
   * subdirectory.
   *
   * @param filesAndDirs the list containing files and directories under a path
   * @param hdfsStats    the hdfs stats of the concerned path
   * @return true if the space_consumed_GB/dir_count ratio is less than the threshold
   */
  @VisibleForTesting
  boolean hasLowROI(FileStatus[] filesAndDirs, HdfsStats hdfsStats) {
    long dirCount = 0;
    for (FileStatus status : filesAndDirs) {
      if (status.isDir()) {
        dirCount++;
      }
    }
    if (dirCount == 0) {
      return false;
    }
    float dirToSpaceRatio = Utils.bytesToGB(hdfsStats.getSpaceConsumed()) / dirCount;

    boolean hasLowROI = Float.compare(dirToSpaceRatio, SPACE_TO_DIR_RATIO) < 0;
    LOG.debug("ROI calculation for path={}, occupied space={}, dirCount={}, dirToSpaceRatio={}, " +
        "hasLowROI={}", new Object[]{hdfsStats.getPath(), Utils.bytesToGB(hdfsStats.getSpaceConsumed())
        , dirCount, dirToSpaceRatio, hasLowROI});

    return hasLowROI;
  }

  @VisibleForTesting
  HdfsStats createStats(String cluster, String path, String owner, ContentSummary summary) {
    return new HdfsStats(
        cluster,
        path,
        owner,
        new DateTime(loadTime),
        summary.getFileCount(),
        summary.getDirectoryCount(),
        summary.getSpaceConsumed(),
        summary.getLength(),
        0L
    );
  }

  public void close() {
    try {
      this.fs.close();
    } catch (IOException e) {
      LOG.warn("Failed to close filesystem", e);
    }
  }

  @Override
  public String toString() {
    return "RPCBasedHdfsStatsBuilder{" +
        "namenode=" + fs.getConf().get(FileSystem.FS_DEFAULT_NAME_KEY) +
        ", runId=" + now +
        ", cluster='" + cluster + '\'' +
        ", loadTime=" + loadTime +
        '}';
  }
}