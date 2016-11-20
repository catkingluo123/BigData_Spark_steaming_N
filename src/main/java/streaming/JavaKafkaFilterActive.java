package streaming;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import redis.RedisClientPool;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/11/16.
 */
public class JavaKafkaFilterActive {

    private static final Logger logger = Logger.getLogger(JavaKafkaFilterActive.class);

    private static final Pattern SPACE = Pattern.compile("\\|");

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaFilterActive <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }


        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaFilterActive");
        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        jssc.checkpoint("/home/hadoop/spark_check");
        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String,Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaMapWithStateDStream<Tuple2<String, String>, Integer, Integer, Tuple2<Tuple2<String ,String>, Integer>> activeStream =
                lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(String x) throws Exception {
                        String[] words = SPACE.split(x);
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        String form_phone = null;
                        String to_phone = null;
                        String day = null;

                        logger.warn(words.toString());

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

        activeStream.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
