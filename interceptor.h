//
// Created by windinsoul on 23-5-31.
//
#ifndef SIMPLEWEIBO_GRPC_INTERCEPTOR_H
#define SIMPLEWEIBO_GRPC_INTERCEPTOR_H
#include<grpcpp/grpcpp.h>
#include "weibo.grpc.pb.h"
#include<sw/redis++/redis++.h>
using weibo::Weibo;

std::unordered_map<grpc::string ,bool> token_box;
std::unordered_map<grpc::string ,bool> no_login={
        {"/weibo.Weibo/Login",1},{"/weibo.Weibo/GetHot",1},{"/weibo.Weibo/PostFeedByRecommend",1},
};

class IfLoginInterceptor:public grpc::experimental::Interceptor{
public:
    IfLoginInterceptor(grpc::experimental::ServerRpcInfo* info):info_(info){}//用一个客户端Rpc请求信息作为初始化

    void Intercept(grpc::experimental::InterceptorBatchMethods* methods)override{
        if(methods->QueryInterceptionHookPoint(
                grpc::experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
//          Hijack all calls
            std::cout<<"Interceptor test, "<<info_->method()<<std::endl;
            if(no_login.find(info_->method())!=no_login.end()&&no_login.find(info_->method())->second==1){//登录请求不拦截
                std::cout<<"a "<<info_->method()<<" RPC"<<std::endl;
                methods->Proceed();
                return;
            }
            auto metadata = methods->GetRecvInitialMetadata();
            bool ok=false;
            if(metadata->find("token")!=metadata->end()&&token_box[grpc::string(metadata->find("token")->second.data(),metadata->find("token")->second.length())]==1)
                ok= true;
            //后面记得改
//            ok=true;
            if(ok){
                std::cout<<"用户验证成功"<<std::endl;
            }else{
                std::cout<<"用户未登录, "<<info_->method()<<"请求不允许"<<std::endl;
                info_->server_context()->TryCancel();
            }
        }
        //拦截器已经完成了对该请求的拦截，继续拦截其它的请求
        methods->Proceed();
    }
private:
    grpc::ServerContext context_;
    std::unique_ptr<Weibo::Stub> stub_;
    grpc::experimental::ServerRpcInfo* info_;
};

//拦截器工程类
class IfLoginInterceptorFactor:public grpc::experimental::ServerInterceptorFactoryInterface{
public:
    grpc::experimental::Interceptor* CreateServerInterceptor(grpc::experimental::ServerRpcInfo* info)override{
        return new IfLoginInterceptor(info);
    }

};
#endif //SIMPLEWEIBO_GRPC_INTERCEPTOR_H
