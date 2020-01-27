package com.stt.spark.dw.realtime.util

import java.util.Objects

import com.alibaba.fastjson.JSON
import com.stt.spark.dw.realtime.bean.StartUpLog
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import org.apache.commons.beanutils.BeanUtils

object MyEsUtil {
    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory:JestClientFactory = null

    /**
    * 获取客户端
    *
    * @return jestclient
    */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
    * 关闭客户端
    */
    def close(client: JestClient): Unit = {
        if (!Objects.isNull(client)) {
            try{
                client.shutdownClient()
            } catch {
                case e: Exception => e.printStackTrace()
            }
        }
    }

    /**
    * 建立连接
    */
    private def build(): Unit = {
        factory = new JestClientFactory
        factory
        .setHttpClientConfig(
           	 	new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
                    .multiThreaded(true)
                    .maxTotalConnection(20) //连接总数
                    .connTimeout(10000)
                    .readTimeout(10000)
                .build
        )
    }

    /**
      * 批量操作
      * @param indexName
      * @param list 由于Index.Builder接收的Any类型，可以传递对象,在bulk操作中，必须使用对象，如果是string需要转换为json对象
      * @param idColumn
      */
    def executeIndexBulk(indexName:String ,list:List[Any], idColumn:String): Unit ={

        val bulkBuilder: Bulk.Builder =
            new Bulk.Builder()
              .defaultIndex(indexName)
              .defaultType("_doc")

        for ( doc <- list ) {
            val indexBuilder = new Index.Builder(doc)
            if(idColumn!=null){
                val id: String = BeanUtils.getProperty(doc,idColumn)
                indexBuilder.id(id)
            }
            bulkBuilder.addAction(indexBuilder.build())
        }
        val jestclient: JestClient =  getClient

        val result: BulkResult = jestclient.execute(bulkBuilder.build())
        if(result.isSucceeded){
            println("保存成功:"+result.getItems.size())
        }else{
            println("保存失败:"+result.getErrorMessage)
        }
        close(jestclient)
    }

    def main(args: Array[String]): Unit = {
        var dsl = "{\n\t\"uid\":\"uid_003\",\n\t\"mid\":\"mid_004\"\n}"
//       var log = StartUpLog("mid003","uid004","apid1","area","os","ch","logType","vs","22","22","33",222L)
        executeIndexBulk("gmall_dau",List(JSON.parseObject(dsl)),null)
//        var jestClient = getClient
//        jestClient.execute(new Index.Builder(dsl).index("gmall_dau").`type`("_doc").build())
//        jestClient.close()
    }


}