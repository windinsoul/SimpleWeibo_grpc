cmake_minimum_required(VERSION 3.10)
project(SimpleWeibo_grpc)

set(CMAKE_CXX_STANDARD 17)
message("-std = c++${CMAKE_CXX_STANDARD}")

find_package(Protobuf REQUIRED)
message(STATUS "Using Protobuf ${Protobuf_VERSION}")
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

add_library(proto_share STATIC weibo.grpc.pb.h weibo.grpc.pb.cc weibo.pb.h weibo.pb.cc)
add_executable(server server.cpp interceptor.h)
add_executable(client client.cpp)

target_link_libraries(server ${PROTOBUF_LIBRARIES} gRPC::grpc++ proto_share mysqlcppconn)
target_link_libraries(client ${PROTOBUF_LIBRARIES} gRPC::grpc++ proto_share)