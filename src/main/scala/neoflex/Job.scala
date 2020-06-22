package neoflex

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.control.Exception.{catching, nonFatalCatcher}

object Job {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stateBackend = new RocksDBStateBackend("file:///home/flink-data/stateTestProject/")
    env.setParallelism(1)
    env.setMaxParallelism(8)
    env.setStateBackend(stateBackend.asInstanceOf[StateBackend])
    env.setRestartStrategy(RestartStrategies.noRestart())

    val dataStream = env
      .socketTextStream("localhost", 9999)
      .keyBy(_.length.toString)
      .map(new MyMapFunction()).uid("testOperator")

    dataStream.print()

    env.execute("State test job")
  }

  class MyMapFunction extends RichMapFunction[String, String] {
    val testListStateDescriptor: ListStateDescriptor[String] = {
      val descriptor = new ListStateDescriptor[String](
        "testStringListState",
        classOf[String]
      )
      descriptor.setQueryable("testQueryableListState")
      descriptor
    }

    val testValueStateDescriptor: ValueStateDescriptor[String] = {
      val descriptor = new ValueStateDescriptor[String](
        "testValueState",
        classOf[String]
      )
      descriptor.setQueryable("testQueryableValueState")
      descriptor
    }

    val testMapStateDescriptor: MapStateDescriptor[String, Int] = {
      val descriptor = new MapStateDescriptor[String, Int](
        "testMapState",
        classOf[String],
        classOf[Int]
      )
      descriptor.setQueryable("testQueryableMapState")
      descriptor
    }

    private var listState: ListState[String] = _
    private var valueState: ValueState[String] = _
    private var mapState: MapState[String, Int] = _

    override def open(parameters: Configuration): Unit = {
      listState = getRuntimeContext.getListState(testListStateDescriptor)
      valueState = getRuntimeContext.getState(testValueStateDescriptor)
      mapState = getRuntimeContext.getMapState(testMapStateDescriptor)
    }

    override def map(value: String): String = {
      catching(nonFatalCatcher).either {
        println(s"Map method. sentence = $value")

        println("list state:")
        listState.get().forEach(println)

        println("value state:")
        println(valueState.value())

        println("map state:")
        mapState.entries().forEach(entry => println(s"Key=${entry.getKey}, value=${entry.getValue}"))

        println("Adding new value to all states:")
        listState.add(value)
        valueState.update(value)
        mapState.put(value, value.length)
      }.left.foreach(e => println(e))
      value
    }
  }
}
