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
package protocol

import (
	"common/utils"
	"fmt"
	"testing"
)

func TestCreateProducerCommand(t *testing.T) {
	qn := "AHSASS./djdd"
	cmd := &CreateProducerCommand{QueueName: qn}
	cmdBytes, err := CommandToBytes(cmd)
	if err != nil {
		t.Error("Failed!")
		return
	}

	cmd1, err1 := BytesToCommand(cmdBytes)
	if err1 != nil {
		t.Error("Failed!")
		return
	}

	cmd2 := cmd1.(*CreateProducerCommand)
	if cmd2.QueueName != qn {
		t.Error("Failed!")
		return
	}
}

func TestCreateConsumerCommand(t *testing.T) {
	qn := "AHSASS./djddASASDDDDDDDDASASDDDDDDDDASASDDDDDDDDASASDDDDDDDD"
	cn := "ASASDDDDDDDDASASDDDDDDDDASASDDDDDDDDASASDDDDDDDDASASDDDDDDDD"
	cmd := &CreateConsumerCommand{QueueName: qn, ConsumerName: cn}
	cmdBytes, err := CommandToBytes(cmd)
	if err != nil {
		t.Error("Failed!")
		return
	}

	cmd1, err1 := BytesToCommand(cmdBytes)
	if err1 != nil {
		t.Error("Failed!")
		return
	}

	cmd2 := cmd1.(*CreateConsumerCommand)
	if cmd2.QueueName != qn {
		t.Error("Failed!")
		return
	}

	if cmd2.ConsumerName != cn {
		t.Error("Failed!")
		return
	}
}

func TestProduceMessageCommand(t *testing.T) {
	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	body := "abcde1234AAsdashjdsabfBBBDHJDvuhasdVVV(*TTF BTVTˆˆT  ^T ÂDADADGDGDG        DGADHADGAD      abcde1234AAsdashjdsabfBBBDHJDvuhasdVVV(*TTF BTVTˆˆT  ^T ÂDADADGDGDG        DGADHADGAD      abcde1234AAsdashjdsabfBBBDHJDvuhasdVVV(*TTF BTVTˆˆT  ^T ÂDADADGDGDG        DGADHADGAD      "
	msgBody1 := []byte(body)
	bytesMsg1, err := NewBytesMessage(msgBody1, seqGen)
	if err != nil {
		fmt.Println("create bytes message failed. error is %+v", err)
		return
	}

	cmd := &ProduceMessageCommand{Message: bytesMsg1}
	cmdBytes, err := CommandToBytes(cmd)
	if err != nil {
		t.Error("Failed!")
		return
	}

	cmd1, err1 := BytesToCommand(cmdBytes)
	if err1 != nil {
		t.Error("Failed!")
		return
	}

	cmd2 := cmd1.(*ProduceMessageCommand)
	msg2 := cmd2.Message.(*BytesMessage)
	body1 := string(msg2.Payload().([]byte))
	if body1 != body {
		t.Error("Failed!")
		return
	}
}

func TestProduceMessageResponse(t *testing.T) {
	var msgId int64 = 1209999
	cmd := &ProduceMessageResponse{MsgId: msgId}
	cmdBytes, err := CommandToBytes(cmd)
	if err != nil {
		t.Error("Failed!")
		return
	}

	cmd1, err1 := BytesToCommand(cmdBytes)
	if err1 != nil {
		t.Error("Failed!")
		return
	}

	cmd2 := cmd1.(*ProduceMessageResponse)
	if cmd2.MsgId != msgId {
		t.Error("Failed!")
		return
	}
}

func TestFailedResponse(t *testing.T) {
	cmd := &FailedResponse{Err: CommandBytesIsNullError}
	cmdBytes, err := CommandToBytes(cmd)
	if err != nil {
		t.Error("Failed!")
		return
	}

	cmd1, err1 := BytesToCommand(cmdBytes)
	if err1 != nil {
		t.Error("Failed!")
		return
	}

	cmd2 := cmd1.(*FailedResponse)
	if cmd2.Err.Error() != cmd.Err.Error() {
		t.Error("Failed!")
		return
	}
}

func TestConsumeMessageResponse(t *testing.T) {
	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	body := "abcde1234AAsdashjdsabfBBBDHJDvuhasdVVV(*TTF BTVTˆˆT  ^T ÂDADADGDGDG        DGADHADGAD      abcde1234AAsdashjdsabfBBBDHJDvuhasdVVV(*TTF BTVTˆˆT  ^T ÂDADADGDGDG        DGADHADGAD      abcde1234AAsdashjdsabfBBBDHJDvuhasdVVV(*TTF BTVTˆˆT  ^T ÂDADADGDGDG        DGADHADGAD      "
	msgBody1 := []byte(body)
	bytesMsg1, err := NewBytesMessage(msgBody1, seqGen)
	if err != nil {
		fmt.Println("create bytes message failed. error is %+v", err)
		return
	}

	cmd := &ConsumeMessageResponse{Message: bytesMsg1}
	cmdBytes, err := CommandToBytes(cmd)
	if err != nil {
		t.Error("Failed!")
		return
	}

	cmd1, err1 := BytesToCommand(cmdBytes)
	if err1 != nil {
		t.Error("Failed!")
		return
	}

	cmd2 := cmd1.(*ConsumeMessageResponse)
	msg2 := cmd2.Message.(*BytesMessage)
	body1 := string(msg2.Payload().([]byte))
	if body1 != body {
		t.Error("Failed!")
		return
	}
}

























