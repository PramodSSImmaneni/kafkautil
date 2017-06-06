package test.kafkautil;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/**
 * Created by pramod on 5/15/17.
 */
public class Util
{

  public static final List<String> SUPPORTED_COMMANDS = Arrays.asList("get-committed-offsets", "set-committed-offsets");

  String topic;
  Properties properties;

  public void getCommittedOffsets()
  {

  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public Properties getProperties()
  {
    return properties;
  }

  public void setProperties(Properties properties)
  {
    this.properties = properties;
  }

  private void getOffsetProperties()
  {
    KafkaConsumer<byte[], byte[]> consumer = createConsumer();
    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
    for (PartitionInfo partitionInfo : partitionInfos) {
      //consumer.position()
    }
  }

  private KafkaConsumer<byte[], byte[]> createConsumer()
  {
    return new KafkaConsumer<byte[], byte[]>(properties);
  }

  public void processCommand(String[] args)
  {
    Options options = new Options();
    options.addOption(Option.builder("p").desc("kafka properties file").hasArg().required().build());
    options.addOption(Option.builder("c").desc("command [" + StringUtils.join(SUPPORTED_COMMANDS,"|") +"]").hasArg().required().build());
    options.addOption(Option.builder("t").desc("topic").hasArg().required().build());
    options.addOption(Option.builder("m").desc("offset map").hasArg().build());

    if (args.length < 2) {
      usage(options, 1);
    }

    CommandLineParser parser = new DefaultParser();
    CommandLine line = null;
    try {
      line = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Error parsing options, " + e.getMessage());
      usage(options, 2);
    }

    String command = line.getOptionValue("c");
    if (!Util.SUPPORTED_COMMANDS.contains(command)) {
      System.err.println("Command " + command + " is not supported");
      usage(options, 3);
    }

    String propFile = line.getOptionValue("p");
    properties = new Properties();

    try {
      try (FileInputStream fis = new FileInputStream(propFile)) {
        properties.load(fis);
      }
    } catch (IOException e) {
      System.err.println("Property file could not be loaded, " + e.getMessage());
      usage(options, 4);
    }

    System.out.println("Properties: " + properties);

    topic = line.getOptionValue("t");
  }

  private void usage(Options options, int errcode)
  {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(Util.class.getName(), options);
    System.exit(errcode);
  }

  public static void main(String[] args)
  {
    new Util().processCommand(args);
  }
}
