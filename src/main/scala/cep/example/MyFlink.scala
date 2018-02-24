package cep.example
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object MyFlink
  {

  def main(args: Array[String]) {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","ip-address:6667,ip-address:6667")
    properties.setProperty("group.id","testcep")
    properties.setProperty("security.protocol", "SASL_PLAINTEXT")
    properties.setProperty("sasl.kerberos.service.name", "kafka")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaEvent = new FlinkKafkaConsumer010[String]("myTopic", new SimpleStringSchema(),properties)
    val text = env.addSource(kafkaEvent)

    env.enableCheckpointing(1000)

    env.setStateBackend(new RocksDBStateBackend("hdfs:///flink/flink-checkpoints/rocksDB/events",true))

    val counts = text.flatMap{_.toLowerCase.split("\\W+") filter { _.nonEmpty }}
      .map{(_,1L)}
      .keyBy(0)
      .flatMap(new MyRichFunction)

    val pattern:Pattern[String,String] = Pattern.begin("start").where((txt) => txt.==("exception"))
    val patternStream:PatternStream[String] = CEP.pattern(text,pattern)
    val result: DataStream[String] = patternStream.select(new PatternSelectFunction[String, String]() {
      override def select(matches: util.Map[String, util.List[String]]): String = {
        if(matches.containsKey("start"))
          throw new Exception
        "Failure generated, Restarting"
      }
    })
    counts.print()
    result.print()
    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      3, // max failures per unit
      org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
      org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS) // delay
    ))
    env.execute("Window Stream WordCount")
  }
}