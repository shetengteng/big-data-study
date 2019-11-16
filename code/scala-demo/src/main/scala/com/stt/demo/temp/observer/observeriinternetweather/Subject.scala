package com.atguigu.temp.observer.observeriinternetweather

trait Subject {

  def registerObserver(o: ObServer)
  def removeObserver(o: ObServer)
  def notifyObservers()

}
