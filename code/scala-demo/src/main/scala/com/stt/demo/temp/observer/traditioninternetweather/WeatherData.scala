package com.atguigu.temp.observer.traditioninternetweather

class WeatherData {


  private var mTemperatrue: Float = _
  private var mPressure: Float = _
  private var mHumidity: Float = _
  private var mCurrentConditions: CurrentConditions = _

  def this(mCurrentConditions: CurrentConditions) {
    this
    this.mCurrentConditions = mCurrentConditions
  }

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
    mCurrentConditions.update(getTemperature(), getPressure(), getHumidity())
  }

  def setData(mTemperature: Float, mPressure: Float, mHumidity: Float) = {
    this.mTemperatrue = mTemperature
    this.mPressure = mPressure
    this.mHumidity = mHumidity
    dataChange()
  }
}
