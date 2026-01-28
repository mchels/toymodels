package main

import (
    "log"
    "net"
    "google.golang.org/grpc"
    "google.golang.org/grpc/reflection"
    "kvstore"
    "kvstore/proto"
)

func main() {

    listener, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatal("Error listening:", err)
    }
    defer listener.Close()

    grpcServer := grpc.NewServer()
    kvServer := kvstore.NewServer()
    proto.RegisterKVStoreServer(grpcServer, kvServer)
    reflection.Register(grpcServer)
    grpcServer.Serve(listener)
}
