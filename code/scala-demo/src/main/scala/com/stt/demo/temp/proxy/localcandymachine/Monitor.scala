package com.atguigu.temp.proxy.localcandymachine

import scala.collection.mutable.ListBuffer

class Monitor {

  private val candyMachinelst: ListBuffer[CandyMachine] = ListBuffer()

  //给监控器增加一个糖果机
  def addMachine(mCandyMachine:CandyMachine) = {
    candyMachinelst.append(mCandyMachine)
  }

  def  report() = {
    //var mCandyMachine:CandyMachine = null
    for (mCandyMachine <- this.candyMachinelst) {
      println("----------------------------------------")
      println("Machine Loc:" + mCandyMachine.getLocation())
      println("Machine Candy count:" + mCandyMachine.getCount())
      println("Machine State:" + mCandyMachine.getstate().getstatename())
    }
  }

}
