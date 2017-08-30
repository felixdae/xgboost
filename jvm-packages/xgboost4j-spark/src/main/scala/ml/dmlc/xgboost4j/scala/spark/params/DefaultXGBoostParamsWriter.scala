/*
 Copyright (c) 2014 by Contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package ml.dmlc.xgboost4j.scala.spark.params

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamPair, Params}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{JObject, _}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

// This originates from apache-spark DefaultPramsWriter copy paste
private[spark] object DefaultXGBoostParamsWriter {

  /**
    * Saves metadata + Params to: path + "/metadata"
    *  - class
    *  - timestamp
    *  - sparkVersion
    *  - uid
    *  - paramMap
    *  - (optionally, extra metadata)
    *
    * @param extraMetadata Extra metadata to be saved at same level as uid, paramMap, etc.
    * @param paramMap      If given, this is saved in the "paramMap" field.
    *                      Otherwise, all [[org.apache.spark.ml.param.Param]]s are encoded using
    *
    */
  def saveMetadata(
                    instance: Params,
                    path: String,
                    sc: SparkContext,
                    extraMetadata: Option[JObject] = None,
                    paramMap: Option[JValue] = None): Unit = {

    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = getMetadataToSave(instance, sc, extraMetadata, paramMap)
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }


  def jsonEncode(value: Any): String = {
    if (value.isInstanceOf[String]) {
      compact(render(JString(value.asInstanceOf[String])))
    } else if (value.isInstanceOf[Vector]) {
      JsonVectorConverter.toJson(value.asInstanceOf[Vector])
    } else {
      throw new NotImplementedError(
        "The default jsonEncode only supports string and vector. ")

    }

    //    value match {
    //      case x: String =>
    //        compact(render(JString(x)))
    //      case v: Vector =>
    //        JsonVectorConverter.toJson(v)
    //      case _ =>
    //        throw new NotImplementedError(
    //          "The default jsonEncode only supports string and vector. " +
    //            s"${this.getClass.getName} must override jsonEncode for ${value.getClass.getName}.")
    //    }
  }

  /**
    * Helper for [[saveMetadata()]] which extracts the JSON to save.
    * This is useful for ensemble models which need to save metadata for many sub-models.
    *
    * @see [[saveMetadata()]] for details on what this includes.
    */
  def getMetadataToSave(
                         instance: Params,
                         sc: SparkContext,
                         extraMetadata: Option[JObject] = None,
                         paramMap: Option[JValue] = None): String = {
    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams = paramMap.getOrElse(render(params.filter {
      case ParamPair(p, _) => p != null
    }.map {
      case ParamPair(p, v) =>
        p.name -> parse(jsonEncode(v))
    }.toList))
    val basicMetadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams)
    val metadata = extraMetadata match {
      case Some(jObject) =>
        basicMetadata ~ jObject
      case None =>
        basicMetadata
    }
    val metadataJson: String = compact(render(metadata))
    metadataJson
  }
}
