
# Any code, applications, scripts, templates, proofs of concept,
# documentation and other items are provided for illustration purposes only.
#
# Copyright 2017 Amazon Web Services
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.8

COPY ./404.go /go/src/github.com/rnzsgh/404/
WORKDIR /go/src/github.com/rnzsgh/404
RUN go get ./ && go build -o 404 . && rm -Rf 404.go && mv 404 /

EXPOSE 80

CMD [ "/404" ]
