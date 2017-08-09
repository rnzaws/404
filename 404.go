/**
 * Any code, applications, scripts, templates, proofs of concept,
 * documentation and other items are provided for illustration purposes only.
 *
 * (C) Copyright 2017 Amazon Web Services
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	log "github.com/sirupsen/logrus"
)

var kinesisStreamName = aws.String(os.Getenv("KINESIS_STREAM_NAME"))

type NotFoundEvent struct {
	Referrer string `json:"referrer"`
	Time     int64  `json:"time"`
}

func main() {

	// Test failure of the container
	/*
		go func() {
			time.Sleep(5 * time.Minute)
			os.Exit(1)
		}()
	*/

	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)

	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	session := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("AWS_REGION")),
	}))

	svc := kinesis.New(session)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "404 Not Found")

		// Test to validate logs being sent to CloudWatch Logs
		log.WithFields(log.Fields{"referrer": r.Referer()}).Info("404 Not Found")

		if r.Referer() == "" {
			return
		}

		notFoundJson, err := json.Marshal(&NotFoundEvent{Referrer: r.Referer(), Time: currentTimeInMillis()})
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("Cannot convert JSON")
			return
		}

		go func(notFoundJson []byte) {
			_, err = svc.PutRecord(&kinesis.PutRecordInput{
				Data:         notFoundJson,
				PartitionKey: aws.String(strconv.Itoa(random.Int())),
				StreamName:   kinesisStreamName,
			})

			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("Unable to insert record in kinesis stream")
			}
		}(notFoundJson)
	})

	http.ListenAndServe(":80", nil)
}

func currentTimeInMillis() int64 {
	tv := new(syscall.Timeval)
	syscall.Gettimeofday(tv)
	return (int64(tv.Sec)*1e3 + int64(tv.Usec)/1e3)
}
