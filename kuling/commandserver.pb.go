// Code generated by protoc-gen-go.
// source: commandserver.proto
// DO NOT EDIT!

/*
Package kuling is a generated protocol buffer package.

It is generated from these files:
	commandserver.proto

It has these top-level messages:
	CreateTopicRequest
	CreateTopicResponse
	PublishRequest
	PublishRequestResponse
*/
package kuling

import proto "github.com/golang/protobuf/proto"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

// CreateTopic request
type CreateTopicRequest struct {
	Topic string `protobuf:"bytes,1,opt,name=topic" json:"topic,omitempty"`
}

func (m *CreateTopicRequest) Reset()         { *m = CreateTopicRequest{} }
func (m *CreateTopicRequest) String() string { return proto.CompactTextString(m) }
func (*CreateTopicRequest) ProtoMessage()    {}

// The response message containing the greetings
type CreateTopicResponse struct {
	Status  int32  `protobuf:"varint,1,opt,name=status" json:"status,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message" json:"message,omitempty"`
}

func (m *CreateTopicResponse) Reset()         { *m = CreateTopicResponse{} }
func (m *CreateTopicResponse) String() string { return proto.CompactTextString(m) }
func (*CreateTopicResponse) ProtoMessage()    {}

// PublishRequest request
type PublishRequest struct {
	Topic   string `protobuf:"bytes,1,opt,name=topic" json:"topic,omitempty"`
	Shard   string `protobuf:"bytes,2,opt,name=shard" json:"shard,omitempty"`
	Key     []byte `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	Payload []byte `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
}

func (m *PublishRequest) Reset()         { *m = PublishRequest{} }
func (m *PublishRequest) String() string { return proto.CompactTextString(m) }
func (*PublishRequest) ProtoMessage()    {}

// PublishRequest request
type PublishRequestResponse struct {
	Status int32 `protobuf:"varint,1,opt,name=status" json:"status,omitempty"`
}

func (m *PublishRequestResponse) Reset()         { *m = PublishRequestResponse{} }
func (m *PublishRequestResponse) String() string { return proto.CompactTextString(m) }
func (*PublishRequestResponse) ProtoMessage()    {}

func init() {
}

// Client API for CommandServer service

type CommandServerClient interface {
	// CreateTopic sends a create topic request down to the log store
	// and echoes the result back to the client
	CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error)
	Publish(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*PublishRequestResponse, error)
}

type commandServerClient struct {
	cc *grpc.ClientConn
}

func NewCommandServerClient(cc *grpc.ClientConn) CommandServerClient {
	return &commandServerClient{cc}
}

func (c *commandServerClient) CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error) {
	out := new(CreateTopicResponse)
	err := grpc.Invoke(ctx, "/kuling.CommandServer/CreateTopic", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *commandServerClient) Publish(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*PublishRequestResponse, error) {
	out := new(PublishRequestResponse)
	err := grpc.Invoke(ctx, "/kuling.CommandServer/Publish", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for CommandServer service

type CommandServerServer interface {
	// CreateTopic sends a create topic request down to the log store
	// and echoes the result back to the client
	CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error)
	Publish(context.Context, *PublishRequest) (*PublishRequestResponse, error)
}

func RegisterCommandServerServer(s *grpc.Server, srv CommandServerServer) {
	s.RegisterService(&_CommandServer_serviceDesc, srv)
}

func _CommandServer_CreateTopic_Handler(srv interface{}, ctx context.Context, buf []byte) (proto.Message, error) {
	in := new(CreateTopicRequest)
	if err := proto.Unmarshal(buf, in); err != nil {
		return nil, err
	}
	out, err := srv.(CommandServerServer).CreateTopic(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _CommandServer_Publish_Handler(srv interface{}, ctx context.Context, buf []byte) (proto.Message, error) {
	in := new(PublishRequest)
	if err := proto.Unmarshal(buf, in); err != nil {
		return nil, err
	}
	out, err := srv.(CommandServerServer).Publish(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _CommandServer_serviceDesc = grpc.ServiceDesc{
	ServiceName: "kuling.CommandServer",
	HandlerType: (*CommandServerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateTopic",
			Handler:    _CommandServer_CreateTopic_Handler,
		},
		{
			MethodName: "Publish",
			Handler:    _CommandServer_Publish_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}
