#!/bin/bash

cd ~/projects/golang/src/github.com/golang/protobuf

protoc  -I ../../fredrikbackstrom/kuling/kuling/broker ../../fredrikbackstrom/kuling/kuling/broker/broker.proto --go_out=plugins=grpc:../../fredrikbackstrom/kuling/kuling/broker

protoc  -I ../../fredrikbackstrom/kuling/kuling ../../fredrikbackstrom/kuling/kuling/commandserver.proto --go_out=plugins=grpc:../../fredrikbackstrom/kuling/kuling
