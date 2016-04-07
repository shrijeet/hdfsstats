package com.hdfsstats;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class StatsFiltersTest {

  RandomHdfsStats hdfsStats = new RandomHdfsStats();

  @Test
  public void testSignificanceBasedFilter() {
    HdfsStats largeSpace = hdfsStats.createOne("dummy", "/path");
    largeSpace.setSpaceConsumed(Long.MAX_VALUE);
    largeSpace.setDirCount(1);
    largeSpace.setFileCount(0);
    assertTrue(StatsFilters.SIGNIFICANCE_BASED_FILTER.accept(largeSpace));

    HdfsStats largeFiles = hdfsStats.createOne("dummy", "/path");
    largeFiles.setSpaceConsumed(1);
    largeFiles.setDirCount(0);
    largeFiles.setFileCount(Long.MAX_VALUE);
    assertTrue(StatsFilters.SIGNIFICANCE_BASED_FILTER.accept(largeFiles));

    HdfsStats largeDirs = hdfsStats.createOne("dummy", "/path");
    largeDirs.setSpaceConsumed(1);
    largeDirs.setDirCount(Long.MAX_VALUE);
    largeDirs.setFileCount(0);
    assertTrue(StatsFilters.SIGNIFICANCE_BASED_FILTER.accept(largeDirs));

    HdfsStats smallSpace = hdfsStats.createOne("dummy", "/path");
    smallSpace.setSpaceConsumed(1);
    smallSpace.setDirCount(0);
    smallSpace.setFileCount(0);
    assertFalse(StatsFilters.SIGNIFICANCE_BASED_FILTER.accept(smallSpace));

    HdfsStats dataDate = hdfsStats.createOne("dummy", "/path/data_date=20131209");
    dataDate.setSpaceConsumed(Long.MAX_VALUE);
    assertFalse(StatsFilters.SIGNIFICANCE_BASED_FILTER.accept(dataDate));

    HdfsStats yyyymmdd = hdfsStats.createOne("dummy", "/path/2013/12/09");
    yyyymmdd.setSpaceConsumed(Long.MAX_VALUE);
    assertFalse(StatsFilters.SIGNIFICANCE_BASED_FILTER.accept(yyyymmdd));
  }

  @Test
  public void testDepthBasedFilter() {
    HdfsStats depthZero = hdfsStats.createOne("dummy", "/");
    HdfsStats depthOne = hdfsStats.createOne("dummy", "/path");
    HdfsStats depthTwo = hdfsStats.createOne("dummy", "/path/path");
    HdfsStats depthThree = hdfsStats.createOne("dummy", "/path/path/path");
    assertTrue(StatsFilters.DEPTH_BASED_FILTER.accept(depthZero));
    assertTrue(StatsFilters.DEPTH_BASED_FILTER.accept(depthOne));
    assertTrue(StatsFilters.DEPTH_BASED_FILTER.accept(depthTwo));
    assertFalse(StatsFilters.DEPTH_BASED_FILTER.accept(depthThree));
  }

}