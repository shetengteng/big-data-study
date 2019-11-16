package com.atguigu.temp.observer.observeriinternetweather

class SinaConditions extends ObServer {
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
    if (this.mTemperature > 50) {
      println("***新浪天气预告 Today mTemperature: " + mTemperature + "* 赶紧离开地球")
    } else if (this.mTemperature > 18 && this.mTemperature < 24) {
      println("***新浪天气预告 Today mTemperature: " + mTemperature + "* 地球还是和适合人类的~")
    } else {
      println("***新浪天气预告 Today mTemperature: " + mTemperature + "***")
    }

    println("***新浪天气预告 Today mPressure: " + mPressure + "***")
    println("***新浪天气预告 Today mHumidity: " + mHumidity + "***")
  }
}
