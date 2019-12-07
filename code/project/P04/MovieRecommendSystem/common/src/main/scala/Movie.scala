package com.stt.recommend


/**
  * movie数据集
  * 格式：mid,name,descri,timelong,issue,shoot,language,genres,actors,directors
  * 260                                                     电影id，mid
  * Star Wars: Episode IV - A New Hope (1977)               电影名称，name
  * Princess Leia is captured and held hostage by the ev    详情描述，descri
  * 121 minutes                                             时长，timelong
  * September 21, 2004                                      发行时间，issue
  * 1977                                                    拍摄时间，shoot
  * English                                                 语言类型，language 每一项用“|”分割
  * Action|Adventure|Sci-Fi                                 类型，genres 每一项用“|”分割
  * Mark Hamill|Harrison Ford|Carrie Fisher|Peter Cushing   演员表，actors 每一项用“|”分割
  * George Lucas                                            导演，directors 每一项用“|”分割
  */

case class Movie(
                  mid: Int,
                  name: String,
                  descri: String,
                  timelong: String,
                  issue: String,
                  shoot: String,
                  language: String,
                  genres: String,
                  actors: String,
                  directors: String
                )