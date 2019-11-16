package com.atguigu.temp.observer.traditioninternetweather

class CurrentConditions {

  private var mTemperature: Float = _
  private var mPressure: Float = _
  private var mHumidity: Float = _

  def display() = {
    println("***Today mTemperature: " + mTemperature + "***")
    println("***Today mPressure: " + mPressure + "***")
    println("***Today mHumidity: " + mHumidity + "***")
  }

  def update(mTemperature: Float, mPressure: Float, mHumidity: Float) = {
    this.mTemperature = mTemperature
    this.mPressure = mPressure
    this.mHumidity = mHumidity
    display()
  }
}
