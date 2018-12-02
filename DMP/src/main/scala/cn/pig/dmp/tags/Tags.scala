package cn.pig.dmp.tags

/**
  * 定义一个打标签的接口
  * 主要是为了定义一种规范，所有的打标签的功能需要继承自该接口
  */
trait Tags {
  /**
    * 定义一个打标签的接口
    * @param args
    * @return
    */
  def makeTags(args:Any*):List[(String,Int)]

}
