/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.iceberg.utils

import scala.reflect.ClassTag

object DelegationUtils {

  /*
   * FT : field type
   * IT : instance type
   * ST : super type
   */


  class DelegatedField[FT, ST : ClassTag, IT <: ST](val instance : IT, fieldName : String) {
    val field = {
      val clz = implicitly[ClassTag[ST]].runtimeClass
      val f = clz.getDeclaredField(fieldName)
      f.setAccessible(true)
      f
    }

    def value : FT = field.get(instance).asInstanceOf[FT]

    def `value_=`(value : FT) : Unit = {
      field.set(instance, value)
    }

  }

  abstract class DelegatedMethod[ST : ClassTag, IT <: ST, RT]
  (instance : IT, methodName : String) {
    val method = {
      val clz = implicitly[ClassTag[ST]].runtimeClass
      val m = clz.getDeclaredMethod(methodName)
      m.setAccessible(true)
      m
    }
  }

  class DelegatedMethod0[ST : ClassTag, IT <: ST, RT](val instance : IT, methodName : String) {

    val method = {
      val clz = implicitly[ClassTag[ST]].runtimeClass
      val m = clz.getDeclaredMethod(methodName)
      m.setAccessible(true)
      m
    }

    def apply : RT = {
      method.invoke(instance).asInstanceOf[RT]
    }
  }

  class DelegatedMethod1[ST : ClassTag, IT <: ST, RT, AT1 : ClassTag]
  (val instance : IT, methodName : String) {

    val method = {
      val clz = implicitly[ClassTag[ST]].runtimeClass
      val m = clz.getDeclaredMethod(methodName, implicitly[ClassTag[AT1]].runtimeClass)
      m.setAccessible(true)
      m
    }

    def apply(arg1 : AT1) : RT = {
      method.invoke(instance, arg1.asInstanceOf[AnyRef]).asInstanceOf[RT]
    }
  }

  class DelegatedMethod2[ST : ClassTag, IT <: ST, RT, AT1 : ClassTag, AT2 : ClassTag]
  (val instance : IT, methodName : String) {

    val method = {
      val clz = implicitly[ClassTag[ST]].runtimeClass
      val m = clz.getDeclaredMethod(methodName,
        implicitly[ClassTag[AT1]].runtimeClass,
        implicitly[ClassTag[AT2]].runtimeClass)
      m.setAccessible(true)
      m
    }

    def apply(arg1 : AT1, arg2 : AT2) : RT = {
      method.invoke(instance, arg1.asInstanceOf[AnyRef], arg2.asInstanceOf[AnyRef]).asInstanceOf[RT]
    }
  }

  // scalastyle:off line.size.limit
  class DelegatedMethod3[ST : ClassTag, IT <: ST, RT, AT1 : ClassTag, AT2 : ClassTag, AT3 : ClassTag]
  (val instance : IT, methodName : String) {

    val method = {
      val clz = implicitly[ClassTag[ST]].runtimeClass
      val m = clz.getDeclaredMethod(methodName,
        implicitly[ClassTag[AT1]].runtimeClass,
        implicitly[ClassTag[AT2]].runtimeClass,
        implicitly[ClassTag[AT3]].runtimeClass)
      m.setAccessible(true)
      m
    }

    def apply(arg1 : AT1, arg2 : AT2, arg3 : AT3) : RT = {
      method.invoke(instance,
        arg1.asInstanceOf[AnyRef],
        arg2.asInstanceOf[AnyRef],
        arg3.asInstanceOf[AnyRef]).asInstanceOf[RT]
    }
  }
  // scalastyle:on line.size.limit

}
