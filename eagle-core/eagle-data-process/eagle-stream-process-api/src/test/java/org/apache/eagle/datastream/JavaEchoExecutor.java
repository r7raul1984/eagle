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
package org.apache.eagle.datastream;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple1;

public class JavaEchoExecutor extends JavaStormStreamExecutor1<String>{
    private static Logger LOG = LoggerFactory.getLogger(JavaEchoExecutor.class);
    private Config config;
    @Override
    public void prepareConfig(Config config){
        this.config = config;
    }

    /**
     * give business code a chance to do initialization for example daemon thread
     * this method is executed in remote machine
     */
    @Override
    public void init(){
    }

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple1<String>> collector){
        collector.collect(new Tuple1(input.get(0)));
        LOG.info("echo " + input);
    }
}
