package com.hdfsstats;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.permission.FsPermission;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;


public class RPCBasedHdfsStatsBuilderTest {
  static final long ONE_HUNDRED_GB = 107374182400L;
  static final int REPLICATION = 3;
  HdfsStatsBuilder hdfsStatsBuilder;
  Path root = new Path("/");
  FileSystem mockFs;
  FileStatus rootStat = statusForPath(root);
  Path subA = new Path("/subA");
  Path subB = new Path("/subA/subB");
  FileStatus subAStat = statusForPath(subA);
  FileStatus subBStat = statusForPath(subB);
  ContentSummary summary = new ContentSummary(ONE_HUNDRED_GB, 1, 1, Long.MAX_VALUE,
      ONE_HUNDRED_GB * REPLICATION, Long.MAX_VALUE);
  RandomHdfsStats randomHdfsStats = new RandomHdfsStats();

  @Before
  public void resetMock() throws IOException {
    mockFs = mock(FileSystem.class);
    when(mockFs.getFileStatus(root)).thenReturn(rootStat);
    when(mockFs.listStatus(root)).thenReturn(new FileStatus[]{subAStat});
    when(mockFs.listStatus(subA)).thenReturn(new FileStatus[]{subBStat});
    when(mockFs.listStatus(subB)).thenReturn(new FileStatus[]{});
    when(mockFs.getContentSummary(any(Path.class))).thenReturn(summary);
    hdfsStatsBuilder = new RPCBasedHdfsStatsBuilder(mockFs, "dummy-cluster",
        Utils.utcDateTime(RandomHdfsStats.DUMMY_RUNID));
  }

  @Test
  public void testAllSignificantDirsAreCovered() throws IOException {
    List<HdfsStats> stats = hdfsStatsBuilder.build();
    assertThat(stats.size(), is(3));
    assertThat(stats, hasItems(
        randomHdfsStats.createOne("dummy-cluster", root.toUri().getPath()),
        randomHdfsStats.createOne("dummy-cluster", subA.toUri().getPath()),
        randomHdfsStats.createOne("dummy-cluster", subB.toUri().getPath()))
    );
  }

  @Test
  public void testSmallDirsAreIgnored() throws IOException {
    //subB has 0 occupied space.
    when(mockFs.getContentSummary(subB)).thenReturn(new ContentSummary());
    List<HdfsStats> stats = hdfsStatsBuilder.build();
    assertThat(stats.size(), is(2));
    assertThat(stats, hasItems(
        randomHdfsStats.createOne("dummy-cluster", root.toUri().getPath()),
        randomHdfsStats.createOne("dummy-cluster", subA.toUri().getPath()))
    );
  }

  @Test
  public void tesLowReturnOfInvestmentOptimization() throws IOException {
    final Path subC = new Path("/subA/subB/subC");
    FileStatus[] lotsOfDirs = createSubDirs(100, subC);
    when(mockFs.listStatus(subB)).thenReturn(new FileStatus[]{statusForPath(subC)});
    when(mockFs.listStatus(subC)).thenReturn(lotsOfDirs);
    when(mockFs.listStatus(argThat(new BaseMatcher<Path>() {
      public boolean matches(Object o) {
        Path path = (Path) o;
        return (path.getParent() != null && path.getParent().equals(subC));
      }
      public void describeTo(Description description) {
      }
    }))).thenReturn(new FileStatus[]{});
    List<HdfsStats> stats = hdfsStatsBuilder.build();
    assertThat(stats.size(), is(4));
  }

  FileStatus[] createSubDirs(int n, Path path) {
    FileStatus[] lotsOfDirs = new FileStatus[n];
    for (int i = 0; i < n; i++) {
      lotsOfDirs[i] = statusForPath(new Path(path, String.valueOf(i)));
    }
    return lotsOfDirs;
  }

  FileStatus statusForPath(Path path) {
    return new FileStatus(0L, true, 0, 0L, 0L, 0, FsPermission.getDefault(),
        RandomHdfsStats.DUMMY_OWNER, RandomHdfsStats.DUMMY_OWNER, path);
  }

}
