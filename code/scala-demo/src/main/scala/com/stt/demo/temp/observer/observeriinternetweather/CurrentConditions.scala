package com.atguigu.temp.observer.observeriinternetweather

class CurrentConditions extends ObServer{


  private var mTemperature: Float = _
  private var mPressure: Float = _
  private var mHumidity: Float = _

  override def update(mTemperatrue: Float, mPressure: Float,
                      mHumidity: Float): Unit = {
    this.mTemperature = mTemperatrue
    this.mPressure = mPressure
    this.mHumidity = mHumidity
    display()
  }
  def display() = {

    println("***公告板 Today mTemperature: " + mTemperature + "***")
    println("***公告板 Today mPressure: " + mPressure + "***")
    println("***公告板 Today mHumidity: " + mHumidity + "***")
  }
}
