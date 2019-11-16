package com.atguigu.temp.observer.observeriinternetweather

object InternetWeather {
  def main(args: Array[String]): Unit = {
    val mCurrentConditions = new CurrentConditions()
    val mWeatherData = new WeatherData()
    mWeatherData.registerObserver(mCurrentConditions)


    val sinaConditions = new SinaConditions()
    mWeatherData.registerObserver(sinaConditions)
    mWeatherData.setData(60,20,30)



  }
}
