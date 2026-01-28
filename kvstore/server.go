package kvstore

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"kvstore/proto"
)

type server struct {
	proto.UnimplementedKVStoreServer
	store *Store
}

func NewServer() server {
	return server{store: NewStore()}
}

func (server server) Get(_ context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	val, ok := server.store.Get(request.Key)
	if !ok {
		return nil, status.Error(codes.NotFound, "Not Found")
	}
	return &proto.GetResponse{Value: val}, nil
}
func (server server) Put(_ context.Context, request *proto.PutRequest) (*proto.PutResponse, error) {
	server.store.Put(request.Key, request.Value)
	return &proto.PutResponse{}, nil
}
func (server server) Delete(_ context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	server.store.Delete(request.Key)
	return &proto.DeleteResponse{}, nil
}
