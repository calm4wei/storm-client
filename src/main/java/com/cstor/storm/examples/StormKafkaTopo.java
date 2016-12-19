package com.cstor.storm.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StormKafkaTopo {

    public static class MessageScheme implements Scheme {

        /* (non-Javadoc)
         * @see backtype.storm.spout.Scheme#deserialize(byte[])
         */
        public List<Object> deserialize(byte[] ser) {
            try {
                String msg = new String(ser, "UTF-8");
                return new Values(msg);
            } catch (UnsupportedEncodingException e) {

            }
            return null;
        }


        /* (non-Javadoc)
         * @see backtype.storm.spout.Scheme#getOutputFields()
         */
        public Fields getOutputFields() {
            // TODO Auto-generated method stub
            return new Fields("msg");
        }
    }


    public static class SenqueceBolt extends BaseBasicBolt {

        /* (non-Javadoc)
         * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
         */
        public void execute(Tuple input, BasicOutputCollector collector) {
            // TODO Auto-generated method stub
            String word = (String) input.getValue(0);
            String out = "I'm " + word + "!";
            System.out.println("out=" + out);
            collector.emit(new Values(out));
        }

        /* (non-Javadoc)
         * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("message"));
        }
    }

    public static void main(String[] args) throws Exception {
        // 配置Zookeeper地址
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "test", "/zkkafkaspout", "kafkaspout");

        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();
        //  KafkaSpout基于kafka.javaapi.consumer.SimpleConsumer实现了consumer客户端的功能，包括 partition的分配，消费状态的维护（offset）
        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout");
        // 给KafkaBolt配置topic及前置tuple消息到kafka的mapping关系
        builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()
                .withTopicSelector(new DefaultTopicSelector("test2"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())
        ).shuffleGrouping("bolt");

        // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();
        Properties props = new Properties();
        // 配置Kafka broker地址, 设置kafka producer的配置
        props.put("metadata.broker.list", "localhost:9092");
        props.put("request.required.acks", "1");
        // serializer.class为消息的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Topo", conf, builder.createTopology());
            Utils.sleep(10000000);
            cluster.killTopology("Topo");
            cluster.shutdown();
        }
    }
}