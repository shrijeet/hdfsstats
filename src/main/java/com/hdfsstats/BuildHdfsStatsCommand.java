package com.hdfsstats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.util.List;

public class BuildHdfsStatsCommand extends Command {

  private static final String NAMENODE = "namenode";
  private static final String CLUSTER = "cluster";

  public Options buildOptions() {
    Options options = new Options();
    options.addOption(OptionBuilder.withDescription(
        "Namenode address")
        .hasArg().isRequired(true).create(NAMENODE));
    options.addOption(OptionBuilder.withDescription(
        "Cluster to which this namenode belongs to")
        .hasArg().isRequired(true).create(CLUSTER));
    return options;
  }

  @Override
  public int run(final CommandLine cl) throws Exception {
    HdfsStatsBuilder builder = new RPCBasedHdfsStatsBuilder(cl.getOptionValue(NAMENODE),
        cl.getOptionValue(CLUSTER), Utils.currentUTCDateTime());
    List<HdfsStats> stats = builder.build();
    HdfsStatsPublisher publisher = new ConsolePublisher();
    publisher.publish(stats);
    return SUCCESS;
  }

  public static void main(String[] args) throws Exception {
    BuildHdfsStatsCommand command = new BuildHdfsStatsCommand();
    command.doMain(args);
  }

}
