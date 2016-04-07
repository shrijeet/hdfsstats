package com.hdfsstats;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class HdfsStats {

  private String cluster;
  private String path;
  private String owner;
  private DateTime loadTime;
  private long fileCount;
  private long dirCount;
  private long spaceConsumed;
  private long size;
  private long accessCount;

  public HdfsStats() {
    // Jackson deserialization
  }

  public HdfsStats(String cluster, String path, String owner, DateTime loadTime,
                   long fileCount, long dirCount, long spaceConsumed, long size,
                   long accessCount) {
    Preconditions.checkState(!Strings.isNullOrEmpty(cluster), "cluster is invalid");
    Preconditions.checkState(!Strings.isNullOrEmpty(path), "path is invalid");
    Preconditions.checkState(!Strings.isNullOrEmpty(owner), "owner is invalid");
    Preconditions.checkNotNull(loadTime, "load time is invalid");
    Preconditions.checkState(loadTime.getZone().equals(DateTimeZone.UTC),
        "Load time should be in UTC, it was " + loadTime.getZone());
    this.cluster = cluster;
    this.path = path;
    this.owner = owner;
    this.loadTime = loadTime;
    this.fileCount = fileCount;
    this.dirCount = dirCount;
    this.spaceConsumed = spaceConsumed;
    this.size = size;
    this.accessCount = accessCount;
  }

  public String getCluster() {
    return cluster;
  }


  public String getPath() {
    return path;
  }

  public String getOwner() {
    return owner;
  }

  public DateTime getLoadTime() {
    return loadTime;
  }

  public long getFileCount() {
    return fileCount;
  }

  public long getDirCount() {
    return dirCount;
  }

  public long getSpaceConsumed() {
    return spaceConsumed;
  }

  public long getSize() {
    return size;
  }

  public long getAccessCount() {
    return accessCount;
  }

  public void setFileCount(long fileCount) {
    this.fileCount = fileCount;
  }

  public void setDirCount(long dirCount) {
    this.dirCount = dirCount;
  }

  public void setSpaceConsumed(long spaceConsumed) {
    this.spaceConsumed = spaceConsumed;
  }

  public void setSize(long size) {
    this.size = size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HdfsStats)) return false;

    HdfsStats stats = (HdfsStats) o;

    if (!cluster.equals(stats.cluster)) return false;
    if (!loadTime.equals(stats.loadTime)) return false;
    if (!owner.equals(stats.owner)) return false;
    if (!path.equals(stats.path)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = cluster.hashCode();
    result = 31 * result + path.hashCode();
    result = 31 * result + owner.hashCode();
    result = 31 * result + loadTime.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "HdfsStats{" +
        "cluster='" + cluster + '\'' +
        ", path='" + path + '\'' +
        ", owner='" + owner + '\'' +
        ", loadTime=" + loadTime +
        ", fileCount=" + fileCount +
        ", dirCount=" + dirCount +
        ", spaceConsumed=" + spaceConsumed +
        ", size=" + size +
        ", accessCount=" + accessCount +
        '}';
  }
}
