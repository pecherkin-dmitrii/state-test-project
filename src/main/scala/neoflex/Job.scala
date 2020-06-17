package neoflex

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.control.Exception.{catching, nonFatalCatcher}

object Job {

  case class TestClass(id: String, info: String)

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stateBackend = new RocksDBStateBackend("file:///home/flink-data/stateTestProject/")
    env.setParallelism(1)
    env.setStateBackend(stateBackend.asInstanceOf[StateBackend])
    env.setRestartStrategy(RestartStrategies.noRestart())

    val dataStream = env
      .socketTextStream("localhost", 9999)
      .keyBy(_.length)
      .map(new MyMapFunction()).uid("testOperator")

    dataStream.print()

    env.execute("State test job")
  }

  class MyMapFunction extends RichMapFunction[String, String] {
    val testModelsListStateDescriptor =
      new ListStateDescriptor[String](
        "testStringListState",
        classOf[String]
      )

    private var listState: ListState[String] = _

    override def open(parameters: Configuration): Unit = {
      listState = getRuntimeContext.getListState(testModelsListStateDescriptor)
    }

    override def map(value: String): String = {
      catching(nonFatalCatcher).either {
        println(s"flatMap method. sentence = $value")
        Thread.sleep(10000)
        println("list state:")
        listState.get().forEach(println)
        listState.add(value)
      }.left.foreach(e => println(e))
      value
    }
  }

}
