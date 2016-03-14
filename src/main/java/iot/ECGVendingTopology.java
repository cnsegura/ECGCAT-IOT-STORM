package iot;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class ECGVendingTopology {
	private static long STORM_KAFKA_READ_FROM_START = -2;
	private static long STORM_KAFKA_READ_FROM_CURRENT_OFFSET = -1;
	private static long readFromMode = STORM_KAFKA_READ_FROM_START;
	private static String TOPIC_NAME = "SensorData";
	
	public static StormTopology buildTopology(OpaqueTridentKafkaSpout spout) throws IOException {
		
		//TridentKafkaConfig spoutConf = new TridentKafkaConfig(hosts, TOPIC_NAME);
		//spoutConf.startOffsetTime = readFromMode;
		//spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		Fields jsonFields = new Fields("Created", "TemperatureinF", "Pressureinmb");
		ExtractTemp eT = new ExtractTemp(85F);
		
		TridentTopology topology = new TridentTopology();
		topology
			.newStream("records", spout)
			.each(new Fields("str"), new JsonProject(jsonFields), jsonFields);
			//.each(new Fields("str"), new ExtractTemp(), new Fields())
		;
		
		return topology.build();
	}
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		
		if(args.length == 1 && args[0].equals("--fromCurrent")) {
			readFromMode = STORM_KAFKA_READ_FROM_CURRENT_OFFSET;
		}
        if (args.length == 0) {
			
	        BrokerHosts hosts = new ZkHosts("localhost:2181");
	        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, "SensorData");
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("ECGVending", conf, buildTopology(kafkaSpout));
        }else{
        	System.err.println("oops");
        }
		
	}
	
}
