#include <iostream>
#include <thread>
#include<grpcpp/grpcpp.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include "weibo.pb.h"
#include "weibo.grpc.pb.h"
#include "threadpool.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::ClientAsyncReader;
using grpc::CompletionQueue;
using grpc::Status;
using grpc::ClientAsyncReaderWriter;

using weibo::Weibo;
using weibo::GetHotPostReq;
using weibo::PostRes;
using weibo::LoginRes;
using weibo::LoginReq;
using weibo::RegistRes;
using weibo::RegistReq;
using weibo::GetPostByUserIdReq;
using weibo::PostFeedReq;
using weibo::PublishPostReq;
using weibo::PublishPostRes;
using weibo::FollowReq;
using weibo::UserInfo;
using weibo::Response;
using weibo::CommentReq;
using weibo::LikeReq;
using weibo::HotTopicRes;
using weibo::Blank;
using weibo::GetPostByTopicIdReq;

//class GetTieClient{
//public:
//    //使用通道channel初始化阻塞式存根stub
//    GetTieClient(std::shared_ptr<Channel> channel):stub_(Weibo::NewStub(channel)){}
//
//    void GetHot(const uint32_t &num){
//        //准备请求
//        GetHotPostReq request;
//        request.set_post_max_rank(num);
//
//        ClientContext context;
//        std::unique_ptr<grpc::ClientReader<PostRes> > reader;
//        //调用RPC接口
//        //我们将上下文和请求传给方法，得到ClientReader返回对象，而不是将上下文，请求和响应传给方法
//        reader= stub_->GetHot(&context,request);
//        std::cout<<"Getting Hot Ties"<<std::endl;
//        PostRes tie;
//        while(reader->Read(&tie)){
//            std::cout<<tie.post_id()<<": "<<tie.text()<<std::endl;
//        }
//        //结束调用
//        Status status=reader->Finish();
//
//    }
//private:
//    std::unique_ptr<Weibo::Stub> stub_;//存根，客户端代理
//};

class GetTieAsyncClient{
public:
    explicit GetTieAsyncClient(std::shared_ptr<Channel> channel)
    :stub_(Weibo::NewStub(channel)){}

    void GetHot(const uint32_t& num=20){
        GetHotPostReq request;
        request.set_post_max_rank(num);

        AsyncGetHotCall* call=new AsyncGetHotCall(num);
        //创建一个读取流
        call->response_reader=stub_->PrepareAsyncGetHot(&call->context,request,&cq_);
        call->response_reader->StartCall((void*)call);//开启读取流，一旦可读，就会加入到cq_中
    }

    void GetHotTopic(const uint32_t& num=20){
        Blank request;

        AsyncGetHotTopicCall* call=new AsyncGetHotTopicCall(num);
        //创建一个读取流
        call->response_reader=stub_->PrepareAsyncGetHotTopic(&call->context,request,&cq_);
        call->response_reader->StartCall((void*)call);//开启读取流，一旦可读，就会加入到cq_中
    }

    void GetPostByTopicId(const int32_t& topic_id){
        GetPostByTopicIdReq request;
        request.set_topic_id(topic_id);

        AsyncGetPostByTopicIdCall* call=new AsyncGetPostByTopicIdCall(topic_id);
        //创建一个读取流
        call->response_reader=stub_->PrepareAsyncGetPostByTopicId(&call->context,request,&cq_);
        call->response_reader->StartCall((void*)call);//开启读取流，一旦可读，就会加入到cq_中
    }

    void PostFeedByFollow(const int &num=10){
        PostFeedReq request;
        request.set_each_flush_post_num(num);
        request.set_token(client_token);

        AsyncPostFeedByFollowCall* call=new AsyncPostFeedByFollowCall(num);

        call->context.AddMetadata("token",client_token);
        call->response_reader=stub_->PrepareAsyncPostFeedByFollow(&call->context,request,&cq_);
        call->response_reader->StartCall(call);
    }

    void PostFeedByRecommend(const int& num=10){
        PostFeedReq request;
        request.set_each_flush_post_num(num);
        request.set_token(client_token);

        AsyncPostFeedByRecommendCall* call=new AsyncPostFeedByRecommendCall(num);
        call->response_reader=stub_->PrepareAsyncPostFeedByRecommend(&call->context,request,&cq_);
        call->response_reader->StartCall(call);
    }

    void GetPostByUserId(const int& user_id){
        GetPostByUserIdReq request;
        request.set_user_id(user_id);

        AsyncGetPostByUserIdCall* call=new AsyncGetPostByUserIdCall(user_id);

        call->response_reader=stub_->PrepareAsyncGetPostByUserId(&call->context,request,&cq_);
        call->response_reader->StartCall(call);
    }

    void Regist(const std::string& user_name,const std::string& password){
        RegistReq request;
        request.set_user_name(user_name);
        request.set_password(password);
        AsyncRegistCall* call=new AsyncRegistCall(user_name,password);
        call->responseReader=stub_->PrepareAsyncRegist(&call->context,request,&cq_);
        call->responseReader->StartCall();
        call->responseReader->Finish(&call->reply,&call->status,call);

    }

    void Login(const std::string& user_name,const std::string& password){
        LoginReq request;
        request.set_user_name(user_name);
        request.set_password(password);
        AsyncLoginCall* call=new AsyncLoginCall(user_name,password);
        call->responseReader=stub_->PrepareAsyncLogin(&call->context,request,&cq_);
        call->responseReader->StartCall();
        call->responseReader->Finish(&call->reply,&call->status,call);
    }

    void PublishPost(const std::string& text){
        PublishPostReq request;
        request.set_posttext(text);
        request.set_token(client_token);
        AsyncPublishPostCall* call=new AsyncPublishPostCall(text);
        call->context.AddMetadata("token",client_token);
        call->responseReader=stub_->PrepareAsyncPublishPost(&call->context,request,&cq_);
        call->responseReader->StartCall();
        call->responseReader->Finish(&call->reply,&call->status,call);
    }

    void Follow(const int& target_user_id){
        FollowReq request;
        request.set_target_user_id(target_user_id);
        request.set_token(client_token);
        AsyncFollowCall* call=new AsyncFollowCall(target_user_id);
        call->context.AddMetadata("token",client_token);
        call->responseReader=stub_->PrepareAsyncFollow(&call->context,request,&cq_);
        call->responseReader->StartCall();
        call->responseReader->Finish(&call->reply,&call->status,call);
    }

    void CommentOnPost(const std::string& comment_text,const int& post_id){
        CommentReq request;
        request.set_comment_text(comment_text);
        request.set_post_id(post_id);
        request.set_token(client_token);
        AsyncCommentOnPostCall* call=new AsyncCommentOnPostCall();
        call->context.AddMetadata("token",client_token);
        call->responseReader=stub_->PrepareAsyncCommentOnPost(&call->context,request,&cq_);
        call->responseReader->StartCall();
        call->responseReader->Finish(&call->reply,&call->status,call);
    }

    void Like(const int& post_id){
        LikeReq request;
        request.set_post_id(post_id);
        request.set_token(client_token);
        AsyncLikeCall* call=new AsyncLikeCall();
        call->context.AddMetadata("token",client_token);
        call->responseReader=stub_->PrepareAsyncLike(&call->context,request,&cq_);
        call->responseReader->StartCall();
        call->responseReader->Finish(&call->reply,&call->status,call);
    }
    //循环监听完成的response
    //输出服务器的response
    void AsyncCompleteRpc(){
        void* got_tag;
        bool ok=false;

        //阻塞直到完成队列cq中有下一个结果可用,因为完成了的rpc请求会返回到cq队列中
        while(cq_.Next(&got_tag,&ok)){
            AsyncRpcCall* call=static_cast<AsyncRpcCall*>(got_tag);//got_tag就知道该完成的请求是哪个发出去的请求啦
//          GPR_ASSERT(ok);//可能失败，请检查网络状态和服务器进程的状态
            if(call!= nullptr){
                if(!ok){
                    call->status=Status::OK;//事件异常断开，强行终止
                    std::cout<<"this call throw,tag: "<<call<<std::endl;
                }
                call->Proceed();
            }
        }
    }

private:
    class AsyncRpcCall{
    public:
        AsyncRpcCall()=default;
//        virtual void* GetResult()=0;
        virtual void Proceed()=0;
        virtual ~AsyncRpcCall(){}
    public:
        ClientContext context;
        Status status= Status(grpc::StatusCode::PERMISSION_DENIED,"finished code.");
    };

    //异步流式接口(单向，客户端接受服务端的流对象)
    //获取热搜的微博
    class AsyncGetHotCall:public AsyncRpcCall{
    public:
        AsyncGetHotCall(int num=20):nums(0),need(num){}
//        void* GetResult()override{
//            return (void*)(&reply);
//        }

        void Proceed()override{
//            std::cout<<"test"<<std::endl;
            if(status.ok()){
                std::cout<<"GetHot rpc stream fetched ended..."<<std::endl;
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;//读取完了，释放
                return;
            }
            nums++;
            if(nums>need){
                status=Status::OK;
                response_reader->Finish(&status,(void*)this);//向完成队列中加入终止读流事件
                std::cout<<"GetHot call,tie text: "<<reply.text()<<" author: "<<reply.author_name()<<std::endl;
                std::cout<<"GetHot call end..."<<std::endl;
            }else{
                if(nums>1){//1的时候才开始将读事件放入队列中
                    std::cout<<"GetHot call,tie text: "<<reply.text()<<" author: "<<reply.author_name()<<std::endl;
                }
                response_reader->Read(&reply,(void*)this);//继续读下一帧
            }
        }
    public:
        int nums;
        int need;
        PostRes reply;
        std::unique_ptr<ClientAsyncReader<PostRes> > response_reader;
    };

    class AsyncGetHotTopicCall:public AsyncRpcCall{
    public:
        AsyncGetHotTopicCall(int num=20):nums(0),need(num){}
//        void* GetResult()override{
//            return (void*)(&reply);
//        }

        void Proceed()override{
            if(status.ok()){
                std::cout<<"GetHotTopic rpc stream fetched ended..."<<std::endl;
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;//读取完了，释放
                return;
            }
            nums++;
            if(nums>need){
                status=Status::OK;
                response_reader->Finish(&status,(void*)this);//向完成队列中加入终止读流事件
                std::cout<<"GetHotTopic call,topic name: "<<reply.topic_name()<<std::endl;
                std::cout<<"GetHotTopic call end..."<<std::endl;
            }else{
                if(nums>1){//1的时候才开始将读事件放入队列中
                    std::cout<<"GetHot call,tie text: "<<reply.topic_name()<<std::endl;
                }
                response_reader->Read(&reply,(void*)this);//继续读下一帧
            }
        }
    public:
        int nums;
        int need;
        HotTopicRes reply;
        std::unique_ptr<ClientAsyncReader<HotTopicRes> > response_reader;
    };

    class AsyncGetPostByTopicIdCall:public AsyncRpcCall{
    public:
        AsyncGetPostByTopicIdCall(int topicid):nums(0),need(20),topic_id(topicid){}
//        void* GetResult()override{
//            return (void*)(&reply);
//        }

        void Proceed()override{
            if(status.ok()){
                std::cout<<"GetPostByTopicId rpc stream fetched ended..."<<std::endl;
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;//读取完了，释放
                return;
            }
            nums++;
            if(nums>need){
                status=Status::OK;
                response_reader->Finish(&status,(void*)this);//向完成队列中加入终止读流事件
                std::cout<<"GetPostByTopicId call,tie text: "<<reply.text()<<" author: "<<reply.author_name()<<std::endl;
                std::cout<<"GetPostByTopicId call end..."<<std::endl;
            }else{
                if(nums>1){//1的时候才开始将读事件放入队列中
                    std::cout<<"GetPostByTopicId call,tie text: "<<reply.text()<<" author: "<<reply.author_name()<<std::endl;
                }
                response_reader->Read(&reply,(void*)this);//继续读下一帧
            }
        }
    public:
        int nums;
        int need;
        int topic_id;
        PostRes reply;
        std::unique_ptr<ClientAsyncReader<PostRes> > response_reader;
    };

    class AsyncPostFeedByFollowCall:public AsyncRpcCall{
    public:
        AsyncPostFeedByFollowCall(int num=10):nums(0),need(num){}

        void Proceed()override{
            if(status.ok()){
                std::cout<<"PostFeedByFollow rpc stream fetched ended..."<<std::endl;
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;//读取完了，释放
                return;
            }
            nums++;
            if(nums>need){
                status=Status::OK;
                response_reader->Finish(&status, this);
                std::cout<<"PostFeedFollow call,tie text: "<<reply.text()<<", author: "<<reply.author_name()<<", publish_time: "<<reply.publish_time()<<std::endl;
                std::cout<<"PostFeedFollow call end,accept last data..."<<std::endl;
            }else{
                if(nums>1){//1的时候才开始将读事件放入队列中
                    std::cout<<"PostFeedFollow call,tie text: "<<reply.text()<<", author: "<<reply.author_name()<<", publish_time: "<<reply.publish_time()<<std::endl;
                }
                response_reader->Read(&reply,(void*)this);//继续读下一帧
            }
        }
    public:
        int nums;
        int need;
        PostRes reply;
        std::unique_ptr<ClientAsyncReader<PostRes> > response_reader;
    };

    class AsyncPostFeedByRecommendCall:public AsyncRpcCall{
    public:
        AsyncPostFeedByRecommendCall(int num=10):nums(0),need(num){}

        void Proceed()override{
            if(status.ok()){
                std::cout<<"PostFeedByRecommend rpc stream fetched ended..."<<std::endl;
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;//读取完了，释放
                return;
            }
            nums++;
            if(nums>need){
                status=Status::OK;
                response_reader->Finish(&status,(void*)this);//向完成队列中加入终止读流事件
                std::cout<<"PostFeedByRecommend call,tie text: "<<reply.text()<<", author: "<<reply.author_name()<<", publish_time: "<<reply.publish_time()<<std::endl;
                std::cout<<"PostFeedByRecommend call end..."<<std::endl;
            }else{
                if(nums>1){//1的时候才开始将读事件放入队列中
                    std::cout<<"PostFeedByRecommend call,tie text: "<<reply.text()<<", author: "<<reply.author_name()<<", publish_time: "<<reply.publish_time()<<std::endl;
                }
                response_reader->Read(&reply,(void*)this);//继续读下一帧
            }
        }
    public:
        int nums;
        int need;
        PostRes reply;
        std::unique_ptr<ClientAsyncReader<PostRes> > response_reader;
    };

    class AsyncGetPostByUserIdCall:public AsyncRpcCall{
    public:
        AsyncGetPostByUserIdCall(const int& user_id):user_id(user_id){}

        void Proceed()override{
            if(status.ok()){
                std::cout<<"GetHot rpc stream fetched ended..."<<std::endl;
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;//读取完了，释放
                return;
            }
            if(nums++==0){
                response_reader->Read(&reply,this);
            }
            else{
                if(nums==1) need=reply.trans_nums();
                if(nums>need){
                    status=Status::OK;
                    response_reader->Finish(&status,(void*)this);//向完成队列中加入终止读流事件
                    std::cout<<"GetPostById ,"<<user_id<<" "<<reply.author_name()<<" "<<reply.text()<<std::endl;
                    std::cout<<"GetPostById call end..."<<std::endl;
                }else{
                    std::cout<<"GetPostById ,"<<user_id<<" "<<reply.author_name()<<" "<<reply.text()<<std::endl;
                    response_reader->Read(&reply,(void*)this);//继续读下一帧
                }
            }
        }

    public:
        int user_id;
        int nums;
        int need;
        PostRes reply;
        std::unique_ptr<ClientAsyncReader<PostRes> > response_reader;
    };

    //注册，单个对象传输接口
    class AsyncRegistCall:public AsyncRpcCall{
    public:
        AsyncRegistCall(const std::string& user_name,const std::string& password)
        :user_name(user_name),password(password){}

        void Proceed()override{
            if(reply.result_code()==0){
                std::cout<<reply.user_name()<<" Regist failed"<<std::endl;
            }else if(reply.result_code()==1){
                std::cout<<reply.user_name()<<" Regist successfully,user_id:"<<reply.user_id()<<std::endl;
            }else if(reply.result_code()==2){
                std::cout<<reply.user_name()<<" already registed!"<<std::endl;
            }
            std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
            delete this;
            return;
        }
    public:
        std::string user_name;
        std::string password;
        RegistRes reply;
        std::unique_ptr<ClientAsyncResponseReader<RegistRes> > responseReader;
    };

    class AsyncLoginCall:public AsyncRpcCall{
    public:
        AsyncLoginCall(const std::string& user_name,const std::string& password)
        :user_name(user_name),password(password){}

        void Proceed()override{
            if(reply.result_code()==0){
                std::cout<<user_name<<" login failed,password is wrong!"<<std::endl;
            }else if(reply.result_code()==1){
                client_token=reply.token();
                std::cout<<"client token is: "<<client_token<<std::endl;
                std::cout<<user_name<<" login successfully, user_id: "<<reply.user_id()<<std::endl;
            }else if(reply.result_code()==2){
                std::cout<<user_name<<" login failed,user is not found,please regist first!"<<std::endl;
            }
            std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
            delete this;
            return;
        }
    public:
        std::string user_name;
        std::string password;
        LoginRes reply;
        std::unique_ptr<ClientAsyncResponseReader<LoginRes> > responseReader;

    };

    class AsyncPublishPostCall:public AsyncRpcCall{
    public:
        AsyncPublishPostCall(const std::string& text)
                :post_text(text){}

        void Proceed()override{
            if(reply.result_code()==0){
                std::cout<<"Publish failed!"<<std::endl;
            }else if(reply.result_code()==1){
                std::cout<<"\033[32mPublish successfully\033[0m"<<std::endl;
            }
            std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
            delete this;
            return;
        }
    public:
        std::string post_text;
        PublishPostRes reply;
        std::unique_ptr<ClientAsyncResponseReader<PublishPostRes> > responseReader;
    };

    class AsyncFollowCall:public AsyncRpcCall{
    public:
        AsyncFollowCall(const int& t_u_id)
                :target_user_id(t_u_id){}

        void Proceed()override{
            if(reply.result_code()==0){
                std::cout<<"Follow failed!"<<std::endl;
            }else if(reply.result_code()==1){
                std::cout<<"\033[32mFollow successfully\033[0m"<<std::endl;
            }
            std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
            delete this;
            return;
        }
    public:
        int target_user_id;
        Response reply;
        std::unique_ptr<ClientAsyncResponseReader<Response> > responseReader;
    };

    class AsyncCommentOnPostCall:public AsyncRpcCall{
    public:
        AsyncCommentOnPostCall()
        {}

        void Proceed()override{
            if(reply.result_code()==0){
                std::cout<<"CommentOnPost failed!"<<std::endl;
            }else if(reply.result_code()==1){
                std::cout<<"\033[32mCommentOnPost successfully\033[0m"<<std::endl;
            }
            std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
            delete this;
            return;
        }
    public:
        Response reply;
        std::unique_ptr<ClientAsyncResponseReader<Response> > responseReader;
    };

    class AsyncLikeCall:public AsyncRpcCall{
    public:
        AsyncLikeCall()
        {}

        void Proceed()override{
            if(reply.result_code()==0){
                std::cout<<"Like failed!"<<std::endl;
            }else if(reply.result_code()==1){
                std::cout<<"\033[32mLike successfully\033[0m"<<std::endl;
            }
            std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
            delete this;
            return;
        }
    public:
        Response reply;
        std::unique_ptr<ClientAsyncResponseReader<Response> > responseReader;
    };

    std::unique_ptr<Weibo::Stub> stub_;
    CompletionQueue cq_;

    static std::string client_token;
};
std::string GetTieAsyncClient::client_token="";
int main(int argc,char** argv)
{
    std::cout<<"Client Starting"<<std::endl;

    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS,20*1000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,10*1000);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,1);



    GetTieAsyncClient client(grpc::CreateCustomChannel("localhost:50051",grpc::InsecureChannelCredentials(),args));
    //生成无限循环的读取器线程
    std::thread thread_=std::thread(&GetTieAsyncClient::AsyncCompleteRpc,&client);
    std::this_thread::sleep_for(std::chrono::seconds (1));
//    client.GetHot(20);
//    client.Regist("ink","123");
//    client.Login("31421","41");
//    client.Login("ink","123");
//    client.Login("ink","321");
//    client.GetPostByUserId(2);
//    client.PostFeedByRecommend();
//    client.Login("ink","123");
//    client.PostFeedByFollow();

    std::string cmd;
    std::cout<<"Press control-c to quit\n"<<std::endl;
    while(std::cin>>cmd){
        if(cmd=="Regist"){
            std::string user_name,password;
            std::cin>>user_name>>password;
            client.Regist(user_name,password);
        }
        if(cmd=="Login") {
            std::string user_name,password;
            std::cin>>user_name>>password;
            client.Login(user_name,password);
        }
        if(cmd=="PostByFollow")
            client.PostFeedByFollow();
        if(cmd=="PostByRecommend")
            client.PostFeedByRecommend();
        if(cmd=="GetHot")
            client.GetHot();
        if(cmd=="PublishPost") {
            std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            client.PublishPost(std::string("hhh") + std::ctime(&now));
            std::cout<<ctime(&now);
        }
        if(cmd=="Follow"){
            int target;
            std::cin>>target;
            client.Follow(target);
        }
        if(cmd=="CommentOnPost"){
            client.CommentOnPost("love",1);
        }
        if(cmd=="GetHotTopic"){
            client.GetHotTopic();
        }
        if(cmd=="GetPostByTopicId"){
            client.GetPostByTopicId(1);
        }
    }
    thread_.join();// 阻塞
    return 0;
}