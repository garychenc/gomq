/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package config

/*
 * Log configuration object
 */
type LogConfig struct {

	/*
	 * The length of the asynchronous log queue when IsAsync is set to true
	 */
	AsyncLogChanLength int64

	/*
	 * Please refer to "github.com/astaxie/beego/logs" package, in the log.go file
	 *
	 */
	Level int

	/*
	 * Please refer to "github.com/astaxie/beego/logs" package, in the log.go file
	 *
	 */
	AdapterName string

	/*
	 * Please refer to "github.com/astaxie/beego/logs" package, in the log.go file
	 *
	 */
	AdapterParameters string

	/*
	 * Is using asynchronous log function
	 */
	IsAsync bool

}
