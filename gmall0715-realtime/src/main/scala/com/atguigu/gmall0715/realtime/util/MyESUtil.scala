package com.atguigu.gmall0715.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyESUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

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
    if (client != null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  /**
    *
    * @param indexName 索引名
    * @param sourceList 要插入的数据源，_1:id, _2:数据
    */
  def insertESBulk(indexName: String, sourceList: List[(String, Any)]): Unit = {
    // 获取ES客户端
    val jest: JestClient = getClient
    // 批量插入
    val bulkBuilder = new Bulk.Builder
    for ((id, source) <- sourceList) {
      val index: Index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
      bulkBuilder.addAction(index)
    }

    // 执行
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
    println("已保存" + items.size() + "条数据")
    close(jest)
  }


  // 测试ES
  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
//    val index: Index = new Index.Builder(new Stu0715("101", "zhang3")).index("stu_0715").`type`("stu").id("101").build()
    val index: Index = new Index.Builder(new Stu0715("102", "li4")).index("stu_0715").`type`("stu").id("102").build()
    jest.execute(index)
    close(jest)
  }

  case class Stu0715(stuId: String, name: String) {

  }

}
