package com.atguigu.temp.observer.observeriinternetweather

import scala.collection.mutable.ListBuffer

class WeatherData extends Subject {


  private var mTemperatrue: Float = _
  private var mPressure: Float = _
  private var mHumidity: Float = _
  private val obServers = ListBuffer[ObServer]()

  def getTemperature() = {
    mTemperatrue
  }

  def getPressure() = {
    mPressure
  }

  def getHumidity() = {
    mHumidity
  }

  def dataChange() = {
    //mCurrentConditions.update(getTemperature(), getPressure(), getHumidity())
    this.notifyObservers()
  }

  def setData(mTemperature: Float, mPressure: Float, mHumidity: Float) = {
    this.mTemperatrue = mTemperature
    this.mPressure = mPressure
    this.mHumidity = mHumidity
    dataChange()
  }

  override def registerObserver(o: ObServer): Unit = {
    obServers.append(o)
  }

  override def removeObserver(o: ObServer): Unit = {
    if (obServers.contains(o)) {
      obServers -= o
    }
  }

  override def notifyObservers(): Unit = {
    for (observer <- obServers) {

      observer.update(getTemperature(),getPressure(),getHumidity())
    }
  }
}
