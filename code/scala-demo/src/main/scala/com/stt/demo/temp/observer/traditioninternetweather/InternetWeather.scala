package com.atguigu.temp.observer.traditioninternetweather

object InternetWeather {
  def main(args: Array[String]): Unit = {
    val mCurrentConditions = new CurrentConditions()
    val mWeatherData = new WeatherData(mCurrentConditions)
    mWeatherData.setData(39, 150, 40)

  }
}
