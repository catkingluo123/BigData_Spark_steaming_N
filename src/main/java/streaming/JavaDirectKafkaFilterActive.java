package streaming;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import redis.RedisClientPool;
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import util.Config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/11/14.
 */
public class JavaDirectKafkaFilterActive {

    private static final Logger logger = Logger.getLogger(JavaDirectKafkaFilterActive.class);

    private static final Pattern SPACE = Pattern.compile("\\t");

    public static void main(String[] args) throws Exception{
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaFilterBlack <topics> <group> <forcefromstart> <numberOfReceivers>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaFilterActive");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        jssc.checkpoint("/home/hadoop/spark_check");
        Properties properties = Config.getConfig("spark_kafka_consumer.properties");
        properties.put("kafka.topic",args[0]);
        properties.put("kafka.consumer.id",args[1]);
        properties.put("consumer.forcefromstart",args[2]);

        logger.warn("properties is " + properties.toString());

        int numberOfReceivers = Integer.parseInt(args[3]);

        JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(
                jssc, properties, numberOfReceivers, StorageLevel.MEMORY_ONLY());

        //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
        JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
                .getPartitionOffset(unionStreams);


        //Start Application Logic
        unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata>>() {
            @Override
            public void call(JavaRDD<MessageAndMetadata> messageAndMetadataJavaRDD) throws Exception {
                List<MessageAndMetadata> rddList = messageAndMetadataJavaRDD.collect();
                logger.warn(" Number of records in this batch " + rddList.size());
            }
        });


        //End Application Logic

        JavaMapWithStateDStream<Tuple2<String, String>, Integer, Integer, Tuple2<Tuple2<String ,String>, Integer>> activeStream = unionStreams.flatMapToPair(
                new PairFlatMapFunction<MessageAndMetadata, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(MessageAndMetadata messageAndMetadata) throws Exception {
                        String line = new String(messageAndMetadata.getPayload());
                        String[] words = SPACE.split(line);
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        String form_phone = null;
                        String to_phone = null;
                        String day = null;

                        if (words.length > 3) {
                            day = words[0].substring(0,8);
                            form_phone = words[2].substring(2,13);
                            to_phone = words[3].substring(2,13);
                        }

                        list.add(new Tuple2<String, String>(form_phone, day));
                        list.add(new Tuple2<String, String>(to_phone, day));

                        return list.iterator();
                    }
                }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String phone = v1._1();
                String day = v1._2();
                Jedis jedis = RedisClientPool.getInstance().getPool().getResource();
                if(jedis.exists(phone)){
                    String reg_time = jedis.get(phone);
                    int timeTest = day.compareTo(reg_time);
                    if(timeTest != -1){
                        return true;
                    }
                }

                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<String,String>, Tuple2<String ,String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String day = stringStringTuple2._2();
                Tuple2<String,String> account = new Tuple2<String, String>(stringStringTuple2._1(),day);
                return new Tuple2<Tuple2<String, String>, Integer>(account ,1);
            }
        }).mapWithState(StateSpec.function(
                new Function3<Tuple2<String, String>, Optional<Integer>, State<Integer>, Tuple2<Tuple2<String, String>, Integer>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> v1, Optional<Integer> one, State<Integer> state) throws Exception {
                        int old = state.exists() ? state.get() : 0;
                        if (old == 0) {
                            // redis put
                            logger.warn("day:" + v1._2() + " phone:" + v1._1());
                            Jedis jedis = RedisClientPool.getInstance().getPool().getResource();
                            jedis.pfadd(v1._2(), v1._1());
                        }
                        int sum = one.or(0) + old;
                        Tuple2<Tuple2<String, String>, Integer> output = new Tuple2<Tuple2<String, String>, Integer>(v1, sum);
                        state.update(sum);
                        return output;
                    }
                }));

        ProcessedOffsetManager.persists(partitonOffset, properties);

        activeStream.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
