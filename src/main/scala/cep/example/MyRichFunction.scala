package cep.example

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

class MyRichFunction extends RichFlatMapFunction[(String,Long),(String,Long)]{

  private var states:ValueState[(String,Long)] = _   //states

  override def flatMap(in:(String,Long), out: Collector[(String,Long)]) : Unit = {
      val temp = states.value()
      val current = if(temp != null){
        temp
      }else{
        (in._1,0L)
      }

      states.update(current._1,current._2+1)

      out.collect(states.value())


  }

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[(String,Long)]("states",createTypeInformation[(String,Long)])
    descriptor.setQueryable("test")
    states = getRuntimeContext.getState(descriptor)
  }

}
