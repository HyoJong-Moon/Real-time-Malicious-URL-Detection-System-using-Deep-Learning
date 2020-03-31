package main.dke.detectURLtopo;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
//import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;

import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.MyKafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class MainTopo {
    private static Log LOG = LogFactory.getLog(MainTopo.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "print help message")
    private boolean _help = false;

    @Option(name = "--topologyName", aliases = {"--name"}, metaVar = "TOPOLOGIE NAME", usage = "name of topology")
    private static String topologyName = "Topo";

    @Option(name = "--inputTopic", aliases = {"--input"}, metaVar = "INPUT TOPIC", usage = "name of input kafka topic")
    private static String inputTopic = "input";

    @Option(name = "--outputTopic", aliases = {"--output"}, metaVar = "OUTPUT TOPIC", usage = "name of output kafka topic")
    private static String outputTopic = "output";

    @Option(name = "--testTime", aliases = {"--t"}, metaVar = "TIME", usage = "how long should run topology")
    private static int testTime = 3;

    @Option(name = "--numWorkers", aliases = {"--workers"}, metaVar = "WORKERS", usage = "number of workers")
    private static int numWorkers = 8;

    @Option(name = "--zookeeperHosts", aliases = {"--zookeeper"}, metaVar = "ZOOKEEPER HOST", usage = "path of zookeeper host")
    private static String zkhosts = "MN:2182,SN01:2182,SN02:2182,SN03:2182,SN04:2182,SN05:2182,SN06:2182,SN07:2182,SN08:2182";

    @Option(name = "--brokerList", aliases = {"--broker"}, metaVar = "BROKER LIST", usage = "path of broker list, bootstrap servers")
    private static String bootstrap = "MN:49092,SN01:49092,SN02:49092,SN03:49092,SN04:49092,SN05:49092,SN06:49092,SN07:49092,SN08:49092";

    @Option(name = "--parallelismHint", aliases = {"--parm"}, metaVar = "PARALLELISM HINT", usage = "number of spout, bolts(KafkaSpout-ExtractBolt-ExpandBolt-ValidateBolt-DetectBolt-KafkaBolt")
    private static String parallelism = "1 2 4 2 2 1";

    @Option(name = "--modelPath", aliases = {"--model"}, metaVar = "TENSORFLOW MODEL PATH", usage = "path of deep learning model")
    private static String modelPath = "./models/";

    @Option(name = "--sourceURL", aliases = {"--source"}, metaVar = "DB SOURCE URL", usage = "Source URL of MariaDB")
    private static String sourceURL = "jdbc:mysql://localhost/test";

    @Option(name = "--tableName", aliases = {"--table"}, metaVar = "DB TABLE NAME", usage = "name of MariaDB table")
    private static String tableName = "test";

    @Option(name = "--dataSourceUser", aliases = {"--user"}, metaVar = "DB USER NAME", usage = "name of MariaDB user")
    private static String dataSourceUser = "user";

    @Option(name = "--dataSourcePassword", aliases = {"--password"}, metaVar = "DB USER PASSWORD", usage = "password of MariaDB user")
    private static String dataSourcePassword = "passwd";

    @Option(name = "--outputDestination", aliases = {"--dst"}, metaVar = "OUTPUT DESTINATION", usage = "destination of Topology")
    private static String destination = "kafka";


    public static void main(String[] args) throws NotAliveException, InterruptedException, TException {
        new MainTopo().topoMain(args);
    }

    public void topoMain(String[] args) throws InterruptedException, NotAliveException, TException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(150);

        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            _help = true;
        }
        if (_help) {
            parser.printUsage(System.err);
            System.err.println();
            return;
        }
        if (numWorkers <= 0) {
            throw new IllegalArgumentException("Need at least one worker");
        }
        if (topologyName == null || topologyName.isEmpty()) {
            throw new IllegalArgumentException("Topology Name must be something");
        }
        // Supporting destination: 'kafka', 'db(database)'
        if (!(destination.equals("kafka") || destination.equals("db"))) {
            throw new IllegalArgumentException("Destination must be kafka or db");
        }

			/* Kafka Spout Configuration */
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, inputTopic, "/" + inputTopic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

			/* KafkaBolt Configuration */
        Properties props = new Properties();
        props.put("metadata.broker.list", bootstrap);
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);
        ExtractionURLBolt extractionBolt = new ExtractionURLBolt();
        ExpansionURLBolt expansionBolt = new ExpansionURLBolt();
        ValidationURLBolt validationBolt = new ValidationURLBolt();
        DetectionBolt detectionBolt = new DetectionBolt(modelPath, destination);
        MyKafkaBolt kafkabolt = null;
        JdbcInsertBolt insertDBBolt = null;

        if (destination.equals("kafka")) {
			/* KafkaBolt */
            kafkabolt = new MyKafkaBolt().withProducerProperties(props)
                    .withTopicSelector(new DefaultTopicSelector(outputTopic))
                    .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        } else if(destination.equals("db")) {

        	/* JDBC Bolt */
            Map hikariConfigMap = Maps.newHashMap();
            hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
            hikariConfigMap.put("dataSource.url", sourceURL);
            hikariConfigMap.put("dataSource.user", dataSourceUser);
            hikariConfigMap.put("dataSource.password", dataSourcePassword);

            ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
            connectionProvider.prepare();


            List<Column> columnSchema = Lists.newArrayList(
                    new Column("text", java.sql.Types.VARCHAR),
                    new Column("url", java.sql.Types.VARCHAR),
                    new Column("result", java.sql.Types.CHAR),
                    new Column("timestamp", java.sql.Types.BIGINT)
            );

            JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);
            insertDBBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                    .withInsertQuery("insert into " + tableName + " (text, url, result, timestamp) values (?,?,?,?)")
                    .withQueryTimeoutSecs(30);
        }
			/* Topology Build */
        TopologyBuilder builder = new TopologyBuilder();

        ArrayList<Integer> parameters = new ArrayList<Integer>();
        String[] params = parallelism.split(" ");

        for (String p : params) {
            parameters.add(Integer.parseInt(p));
        }

        builder.setSpout("kafka-spout", kafkaSpout, parameters.get(0));
        builder.setBolt("extract-bolt", extractionBolt, parameters.get(1)).shuffleGrouping("kafka-spout");
        // builder.setBolt("expand-bolt", expansionBolt, parameters.get(2)).shuffleGrouping("extract-bolt");
        //builder.setBolt("validate-bolt", validationBolt, parameters.get(3)).shuffleGrouping("expand-bolt");
        builder.setBolt("detect-bolt", detectionBolt, parameters.get(4)).shuffleGrouping("extract-bolt");
        if (destination.equals("db")) {
            builder.setBolt("db-bolt", insertDBBolt, parameters.get(5)).shuffleGrouping("detect-bolt");
        } else if (destination.equals("kafka")) {
            builder.setBolt("kafka-bolt", kafkabolt, parameters.get(5)).    shuffleGrouping("detect-bolt");            // Store Data to Kafka
        }

        Config config = new Config();
        config.setNumWorkers(numWorkers);

        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        printSystem();

        try {
            Thread.sleep(testTime * 60 * 1000);

            Map<String, Object> conf = Utils.readStormConfig();
            Client client = NimbusClient.getConfiguredClient(conf).getClient();
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(0);
            client.killTopologyWithOpts(topologyName, killOpts);
        } catch (AlreadyAliveException ae) {
            LOG.info(ae.get_msg());
        } catch (InvalidTopologyException ie) {
            LOG.info(ie.get_msg());
        }
    }

    public static void printSystem() {
        System.out.println("");
        System.out.println("");
        System.out.println("   #     #     #     #        ###   #####   ###  #######  #     #   #####	#     #  ######   #");
        System.out.println("   ##   ##    # #    #         #   #     #   #   #     #  #     #  #     	#     #  #     #  #");
        System.out.println("   #  #  #  #     #  #         #   #         #   #     #  #     #   #####	#     #  ######   #");
        System.out.println("   #     #  #######  #         #   #         #   #     #  #     #       #	#     #  #   #    #");
        System.out.println("   #     #  #     #  #######  ###   #####   ###  #######   #####    #####	 #####   #     #  #######");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("   ######  ###### #######  ######   #####   #######  ###  #######  #     #     #####  #   #  #####  #######  ######  #     #");
        System.out.println("   #     # #         #     #        #          #      #   #     #  # #   #    #        # #   #         #     #       ##   ##");
        System.out.println("   #     # #####     #     #####    #          #      #   #     #  #  #  #     #####    #    #####     #     #####   #  #  #");
        System.out.println("   #     # #         #     #        #          #      #   #     #  #   # #          #   #         #    #     #       #     #");
        System.out.println("   ######  ######    #     ######   #####      #     ###  #######  #     #     #####    #    #####     #     ######  #     #");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
    }
}