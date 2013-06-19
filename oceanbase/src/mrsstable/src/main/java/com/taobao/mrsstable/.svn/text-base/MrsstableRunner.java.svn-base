package com.taobao.mrsstable;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * A utility to help run MRGenSstable class. It is as the same as ToolRunner,
 * exclude the MrsstableOptionsParser is used instead of GenericOptionsParser
 * 
 * @see Tool
 * @see ToolRunner
 * @see GenericOptionsParser
 */
public class MrsstableRunner {
  private static final MRLogger LOG = MRLogger.getLogger(MrsstableRunner.class);

  /**
   * add mrsstable additional options
   * 
   * @return
   */
  @SuppressWarnings("static-access")
  private static Options buildMrsstableOptions() {
    Options options = new Options();
    Option tableId = OptionBuilder.withArgName("table id").hasArg()
        .withDescription("specify the id of the table to run mrsstable tool.").create("table_id");
    Option rowkeyDesc = OptionBuilder
        .withArgName("rowkey desc")
        .hasArg()
        .withDescription(
            "rowkey description, it defines the rowkey formation"
                + " format = \"clomn index in rowkey\",\"column index in origin line\",\"the column type\"")
        .create("rowkey_desc");
    Option sampler = OptionBuilder.withArgName("sampler name").hasArg()
        .withDescription("specify sampler file: specify, random, interval, split.").create("sampler");
    options.addOption(tableId);
    options.addOption(rowkeyDesc);
    options.addOption(sampler);
    return options;
  }

  /**
   * overload the param of configuration file(specified with -conf)
   * 
   * @param conf
   * @param line
   * @throws IOException
   */
  private static void processMrsstableOptions(Configuration conf, CommandLine line) throws IOException {
    if (line.hasOption("table_id")) {
      String value = line.getOptionValue("table_id");
      conf.set("mrsstable.table.id", value);
      LOG.info("overload mrsstable.table.id with cmd opertion table_id: " + value);
    }

    if (line.hasOption("rowkey_desc")) {
      String value = line.getOptionValue("rowkey_desc");
      conf.set("mrsstable.rowkey.desc", value);
      LOG.info("overload mrsstable.rowkey.desc with cmd opertion rowkey_desc: " + value);
    }

    if (line.hasOption("sampler")) {
      String value = line.getOptionValue("sampler");
      conf.set("mrsstable.presort.sampler", value);
      LOG.info("overload mrsstable.presort.sampler with cmd opertion sampler: " + value);
    }
  }

  public static int run(Configuration conf, Tool tool, String[] args) throws Exception {
    if (conf == null) {
      conf = new Configuration();
    }

    Options options = buildMrsstableOptions();

    GenericOptionsParser parser = new GenericOptionsParser(conf, options, args);
    CommandLine commandLine = parser.getCommandLine();
    processMrsstableOptions(conf, commandLine);

    tool.setConf(conf);

    String[] toolArgs = parser.getRemainingArgs();
    return tool.run(toolArgs);
  }

  public static int run(Tool tool, String[] args) throws Exception {
    return run(tool.getConf(), tool, args);
  }

  public static void printMrsstableCommandUsage(PrintStream out) {
    out.printf("Usage: %s [options] <input dirs> <output dir>\n", MrsstableRunner.class.getSimpleName());
    out.println("options supported are (overload the param of configuration file, which is specified with -conf) : ");
    out.println("-table_id <table id>           overload mrsstable.table.id");
    out.println("-rowkey_desc <rowkey desc>     overload mrsstable.rowkey.desc");
    out.println("-sampler <sampler type>        overload mrsstable.presort.sampler");

    GenericOptionsParser.printGenericCommandUsage(out);
  }

  public static void main(String[] args) throws Exception {
    int res = MrsstableRunner.run(new Configuration(), new MRGenSstable(), args);
    System.exit(res);
  }
}
