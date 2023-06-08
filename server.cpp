#include <iostream>
#include<string>
#include<algorithm>

#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>

#include<grpcpp/grpcpp.h>
#include "weibo.pb.h"
#include "weibo.grpc.pb.h"
#include "component.h"
#include "interceptor.h"
#include "threadpool.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerCompletionQueue;

using weibo::GetHotPostReq;
using weibo::Weibo;
using weibo::PostRes;
using weibo::RegistReq;
using weibo::RegistRes;
using weibo::LoginReq;
using weibo::LoginRes;
using weibo::GetPostByUserIdReq;
using weibo::PostFeedReq;
using weibo::PublishPostReq;
using weibo::PublishPostRes;

std::unordered_map<std::string,int> Token2Id={{"this is token",1}};

//Mysql连接类
class MySQLConnector {
public:
    MySQLConnector(const std::string& host, const std::string& user, const std::string& password, const std::string& database)
            : host_(host), user_(user), password_(password), database_(database) {
        driver_ = sql::mysql::get_mysql_driver_instance();
        connection_ = driver_->connect(host_, user_, password_);
        connection_->setSchema(database_);
        std::cout<<"\033[32mConnect Database weibo_grpc successfully\033[0m"<<std::endl;
    }

    ~MySQLConnector() {
        delete connection_;
    }

    bool executeQuery(const std::string& query) {
        try {
            sql::Statement* statement = connection_->createStatement();
            statement->execute(query);
            delete statement;
            return true;
        } catch (sql::SQLException& e) {
            std::cout << "SQLException: " << e.what() << std::endl;
            return false;
        }
    }

    sql::ResultSet* executeQueryWithResult(const std::string& query) {
        try {
            sql::Statement* statement = connection_->createStatement();
            sql::ResultSet* result = statement->executeQuery(query);
            delete statement;
            return result;
        } catch (sql::SQLException& e) {
            std::cout << "SQLException: " << e.what() << std::endl;
            return nullptr;
        }
    }

    sql::Connection* GetCon(){
        return connection_;
    }

private:
    std::string host_;
    std::string user_;
    std::string password_;
    std::string database_;

    sql::mysql::MySQL_Driver* driver_;
    sql::Connection* connection_;
};

MySQLConnector connector("8.134.129.141", "weibo_grpc", "b3SHPHyJ5xpGZkEH", "weibo_grpc");
sql::PreparedStatement* stmt;
sql::Connection* con=connector.GetCon();

class AsyncServerImpl final {
public:
    enum CallStatus{CREATE,PROCESS,FINISH};

    ~AsyncServerImpl(){
        server_->Shutdown();
        cq_->Shutdown();
    }

    //刷新公共帖子池
    static void FlushCommonPostsPool(){
        cpp_len=20;cpp_id=0;
        for(int i=0;i<20;i++){
            PostRes x;
            x.set_author_name(grpc::to_string(i));
            x.set_text("I am "+ grpc::to_string(i));
            common_posts_pool.emplace_back(std::move(x));
        }
    }

    static std::vector<PostRes> GetSubVectorFromCommonPool(int get_len=10){
        std::vector<PostRes> res;
        for(int i=cpp_id;i<cpp_id+get_len&&i<cpp_len;i++){
            res.emplace_back(common_posts_pool[i]);
        }
        if(cpp_id==cpp_len)
            FlushCommonPostsPool();
        return res;
    }

    void Run(){
        std::string server_address("0.0.0.0:50051");

        ServerBuilder builder;
        builder.AddListeningPort(server_address,grpc::InsecureServerCredentials());
        //拦截器
        std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface> > interceptor_creators;
        interceptor_creators.push_back(std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>(new IfLoginInterceptorFactor()));
        builder.experimental().SetInterceptorCreators(std::move(interceptor_creators));//unique_ptr没有默认的拷贝和复制操作

         //心跳探活
         //发送探活的间隔
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS,20*1000);
        //发送方等待探活返回确认的时间，如果超过事件内则断开连接
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,10*1000);
        //没有请求也可以发送探活
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,1);
        //如果传输上没有发送数据/标头帧，则服务器端的此通道参数控制 gRPC Core 在接收连续 ping 之间期望的最短时间
        //如果连续没有数据的ping的间隔时间小于此时间，则 ping 将被视为来自对等方的不良 ping。
        // 这样的 ping 算作“ping 攻击”。在客户端，这没有任何效果
        builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS,10*1000);
        //多线程：动态调整epoll线程数量
        builder.SetSyncServerOption(ServerBuilder::MIN_POLLERS,4);
        builder.SetSyncServerOption(ServerBuilder::MAX_POLLERS,8);

        builder.RegisterService(&service_);
        cq_=builder.AddCompletionQueue();
        //开始部署服务器
        server_=builder.BuildAndStart();
        std::cout<<"Server listening on "<<server_address<<std::endl;

        //事件循环
        HandelRpc();
    }
private:
    class CallData{
    public:
        CallData(Weibo::AsyncService* service,ServerCompletionQueue* cq)
        :service_(service),cq_(cq),status_(CREATE){
            Proceed();
        }
        virtual void Proceed(){}
    public:
        CallStatus status_;
        ServerContext ctx_;
        Weibo::AsyncService* service_;
        ServerCompletionQueue* cq_;
    };

    class GetHotCall:public CallData{//服务端写入流
    public:
        GetHotCall(Weibo::AsyncService* service,ServerCompletionQueue* cq,int nums=20)
        : CallData(service,cq), GetHotRes_writer(&ctx_),need(nums){
            Proceed();
        }

        void Proceed()override{
            GetHotTie_proceed();
        }
    private:
        void GetHotTie_proceed(){
            if(status_==CREATE){//CREATE
                status_=PROCESS;
                //作为初始CREATE状态的一部分，我们请求系统开始处理请求。

                //在这个请求中，“this”操作是唯一标识请求的标签,
                //这样不同的CallData实例可以同时服务于不同的请求，在这种情况下是这个CallData实例的内存地址。
                service_->RequestGetHot(&ctx_,&request,&GetHotRes_writer,cq_,cq_, this);//放入队列中
            }else if(status_==PROCESS){//PROCESS
                //生成一个新的CallData实例来为新的客户端提供服务，
                if(num==0) {
                    new GetHotCall(service_, cq_, need);
                    /*获取热搜列表*/
                    QueryPostByVisits(HotPostList);
                }

                if(++num>std::min(need,(int)HotPostList.size())){
                    status_=FINISH;
                    GetHotRes_writer.Finish(Status::OK, this);//次数超过need，终止写入流
                }else{
                    PostRes response;
                    std::cout<<"Handle GetHotTieCall, "<<num<<std::endl;
                    /*handle database*/

                    response=HotPostList[num-1];

                    GetHotRes_writer.Write(response,this);
                }
            }else{//FINISH状态，释放该CallData实例(一个客户端的一次rpc访问)
                GPR_ASSERT(status_==FINISH);
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;
            }
        }

        int num;
        int need;
        std::vector<PostRes> HotPostList;
        GetHotPostReq request;
        grpc::ServerAsyncWriter<PostRes> GetHotRes_writer;

    };

    class GetPostByUserIdCall:public CallData{
    public:
        GetPostByUserIdCall(Weibo::AsyncService* service,ServerCompletionQueue* cq)
        : CallData(service,cq), responder_writer(&ctx_){
            Proceed();
        }

        void Proceed()override{
            if(status_==CREATE){
                status_=PROCESS;

                service_->RequestGetPostByUserId(&ctx_,&request,&responder_writer,cq_,cq_,this);
            }else if(status_==PROCESS){
                if(count++==0){
                    new GetPostByUserIdCall(service_,cq_);


                    QueryPostByUserId(PostList,request.user_id());
                    maxx=PostList.size();
                }
                if(count>maxx){
                    status_ = FINISH;
                    responder_writer.Finish(Status::OK,this);
                }else{
                    std::cout<<count<<" Handle GetPostByUserIdCall, user_id: "<<request.user_id()<<std::endl;
                    if(count==1) PostList[0].set_trans_nums(maxx);
                    responder_writer.Write(PostList[count-1], this);
                }
            }else{//FINISH
                GPR_ASSERT(status_==FINISH);
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;
            }
        }

    private:
        uint32_t user_id;
        int count=0;
        int maxx=0;
        std::vector<PostRes> PostList;
        GetPostByUserIdReq request;
        grpc::ServerAsyncWriter<PostRes> responder_writer;
    };

    class RegistCall:public CallData{//单个对象
    public:
        RegistCall(Weibo::AsyncService* service,ServerCompletionQueue* cq):
                CallData(service,cq),responder_(&ctx_){
            Proceed();
        }

        void Proceed()override{
            if(status_==CREATE){
                status_=PROCESS;
                service_->RequestRegist(&ctx_,&request_,&responder_,cq_,cq_,this);
            }else if(status_==PROCESS){
                new RegistCall(service_,cq_);

                std::string user_name(request_.user_name());
                std::string password(request_.password());
                std::cout<<"Handle RegistCall for user: "+user_name<<std::endl;

                //db操作
                std::lock_guard<std::mutex> lock(db_mutex);
                stmt=con->prepareStatement("SELECT count(*) FROM `user` WHERE user_name=?");
                stmt->setString(1,user_name);
                sql::ResultSet* resultSet=stmt->executeQuery();
                if(resultSet->next()){
                    if(resultSet->getInt(1)==0){//注册
                        stmt = con->prepareStatement("INSERT INTO `user`(user_name,password) VALUES(?,?)");
                        stmt->setString(1, user_name); // 将用户名作为参数
                        stmt->setString(2, password); // 将密码作为参数
                        if(stmt->executeUpdate()==1) { //插入成功
                            reply_.set_result_code(1);
                            stmt=con->prepareStatement("SELECT id FROM `user` WHERE user_name=?");
                            stmt->setString(1,user_name);
                            sql::ResultSet* result=stmt->executeQuery();
                            if(result->next())
                                reply_.set_user_id(result->getInt("id"));
                            std::cout << user_name << " regist successfully" <<" user_id:"<<reply_.user_id()<< std::endl;
                        }
                        else{//插入失败
                            reply_.set_result_code(0);
                            std::cout<<user_name<<" regist failed"<<std::endl;
                        }
//                        delete stmt;
                    }else{//注册过了
                        reply_.set_result_code(2);
                        std::cout<<user_name<<"already registed!"<<std::endl;
                    }
                }
                reply_.set_user_name(user_name);

                status_=FINISH;
                responder_.Finish(reply_,Status::OK,this);
            }else{//FINISH
                GPR_ASSERT(status_==FINISH);
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;
            }
        }

    private:
        RegistReq request_;
        RegistRes reply_;
        grpc::ServerAsyncResponseWriter<RegistRes> responder_;
    };

    class LoginCall:public CallData{//单个对象
    public:
        LoginCall(Weibo::AsyncService* service,ServerCompletionQueue* cq)
        : CallData(service,cq),responder_(&ctx_){
            Proceed();
        }

        void Proceed()override{
                if(status_==CREATE){
                    status_=PROCESS;
                    service_->RequestLogin(&ctx_,&request_,&responder_,cq_,cq_, this);
                }else if(status_==PROCESS){
                    new LoginCall(service_,cq_);

                    std::string user_name(request_.user_name());
                    std::string password(request_.password());
                    std::cout<<"Handle LoginCall for user: "+user_name<<std::endl;

                    //db操作
                    stmt=con->prepareStatement("SELECT * FROM `user` WHERE user_name=?");
                    stmt->setString(1,user_name);
                    sql::ResultSet* resultSet=stmt->executeQuery();
                    if(resultSet->next()){
                        if(password==resultSet->getString("password")){
                            std::cout<<user_name<<" login successfully"<<std::endl;
                            stmt=con->prepareStatement("SELECT id FROM `user` WHERE user_name=?");
                            stmt->setString(1,user_name);
                            sql::ResultSet* result=stmt->executeQuery();
                            if(result->next())
                                reply_.set_user_id(result->getInt("id"));
                            reply_.set_result_code(1);
                            std::string tmp= CalculateMD5(request_.password());
                            reply_.set_token(tmp);
                            Token2Id.insert({tmp,reply_.user_id()});
                            token_box.insert({tmp,1});
//                            token_box.insert({grpc::string(tmp),1});
                            std::cout<<"Login return a token: "<<tmp<<std::endl;
                        }else{
                            std::cout<<"password is wrong!"<<std::endl;
                            reply_.set_result_code(0);
                        }
                    }else{//用户不存在
                        std::cout<<"user is not found,please regist first!"<<std::endl;
                        reply_.set_result_code(2);
                    }

                    status_=FINISH;
                    responder_.Finish(reply_,Status::OK,this);
                }else{
                    GPR_ASSERT(status_==FINISH);
                    std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                    delete this;
                }
        }

    private:
        LoginReq request_;
        LoginRes reply_;
        grpc::ServerAsyncResponseWriter<LoginRes> responder_;
    };

    class PublishPostCall:public CallData{
    public:
        PublishPostCall(Weibo::AsyncService* service,ServerCompletionQueue* cq)
        : CallData(service,cq),responder_(&ctx_){
                Proceed();
        }

        void Proceed()override{
            if(status_==CREATE){
                status_=PROCESS;
                service_->RequestPublishPost(&ctx_,&request_,&responder_,cq_,cq_, this);
            }else if(status_==PROCESS){
                new PublishPostCall(service_,cq_);

                if(request_.token()==""){status_=FINISH;std::cout<<"no token,call cancel!"<<std::endl;return;}
                std::cout<<request_.token()<<std::endl;
                int user_id=Token2Id[request_.token()];
                std::cout<<"Handle PublishPost for user: "+user_id<<std::endl;

                //db操作
                std::lock_guard<std::mutex> lock(db_mutex);
                stmt=con->prepareStatement("INSERT INTO post(post.author_id,post.text,post.publish_time) VALUES(?,?,CURRENT_TIMESTAMP)");
                stmt->setInt(1,user_id);
                stmt->setString(2,request_.posttext());
                int do_rows=stmt->executeUpdate();
                if(do_rows==1){
                    std::cout<<"\033[32mPublishPost Insert successfully\033[0m"<<std::endl;
                    reply_.set_result_code(1);
                }
                status_=FINISH;
                responder_.Finish(reply_,Status::OK,this);
            }else{
                GPR_ASSERT(status_==FINISH);
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;
            }
        }

    private:
        PublishPostReq request_;
        PublishPostRes reply_;
        grpc::ServerAsyncResponseWriter<PublishPostRes> responder_;
    };

    //刷推荐
    class PostFeedByRecommend:public CallData{
    public:
        PostFeedByRecommend(Weibo::AsyncService* service,ServerCompletionQueue* cq)
        : CallData(service,cq), PostRecommend_writer(&ctx_){
            Proceed();
        }

        void Proceed()override{
            PostFeedByRecommend_proceed();
        }
    private://分登录和未登录(暂时做未登录的)
        void PostFeedByRecommend_proceed(){
            if(status_==CREATE){//CREATE
                status_=PROCESS;
                //作为初始CREATE状态的一部分，我们请求系统开始处理请求。

                //在这个请求中，“this”操作是唯一标识请求的标签,
                //这样不同的CallData实例可以同时服务于不同的请求，在这种情况下是这个CallData实例的内存地址。
                service_->RequestPostFeedByRecommend(&ctx_,&request,&PostRecommend_writer,cq_,cq_,this);
            }else if(status_==PROCESS){//PROCESS
                //生成一个新的CallData实例来为新的客户端提供服务，
                if(num==0) {
                    new PostFeedByRecommend(service_, cq_);
                    //请求来咯
                    post_list= GetSubVectorFromCommonPool();
                }

                if(++num>std::min(need,(int)post_list.size())){
                    status_=FINISH;
                    PostRecommend_writer.Finish(Status::OK, this);//次数超过need，终止写入流
                }else{
                    PostRes response;
                    std::cout<<"Handle PostFeedByRecommendCall, "<<num<<std::endl;
                    /*handle database*/

                    response=post_list[num-1];

                    PostRecommend_writer.Write(response,this);
                }
            }else{//FINISH状态，释放该CallData实例(一个客户端的一次rpc访问)
                GPR_ASSERT(status_==FINISH);
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;
            }
        }

        int num;
        int need=10;
//        std::vector<PostRes> HotPostList;
        int user_id=0;
        PostFeedReq request;
        grpc::ServerAsyncWriter<PostRes> PostRecommend_writer;
        std::vector<PostRes> post_list;
    };

    //刷关注
    class PostFeedByFollowCall:public CallData{
    public:
        PostFeedByFollowCall(Weibo::AsyncService* service,ServerCompletionQueue* cq)
                : CallData(service,cq), serverAsyncWriter(&ctx_){
            Proceed();
        }

        void Proceed()override{
            PostFeedByFollow_proceed();
        }
    private:
        void PostFeedByFollow_proceed(){
            if(status_==CREATE){//CREATE
                status_=PROCESS;
                //作为初始CREATE状态的一部分，我们请求系统开始处理请求。

                //在这个请求中，“this”操作是唯一标识请求的标签,
                //这样不同的CallData实例可以同时服务于不同的请求，在这种情况下是这个CallData实例的内存地址。
                service_->RequestPostFeedByFollow(&ctx_,&request,&serverAsyncWriter,cq_,cq_,this);
            }else if(status_==PROCESS){//PROCESS
                //生成一个新的CallData实例来为新的客户端提供服务，
                if(num==0) {
                    new PostFeedByFollowCall(service_, cq_);
                    //请求来咯
                    std::cout<<"token: "<<request.token()<<std::endl;
                    int user_id= Token2Id[request.token()];
                    std::cout<<"user_id: "<<user_id<<std::endl;
                    QueryPostByFollow(post_list,user_id);
                    std::cout<<"post_list length: "<<post_list.size()<<std::endl;
                }

                if(++num>std::min(need,(int)post_list.size())){
                    status_=FINISH;
                    serverAsyncWriter.Finish(Status::OK, this);//次数超过need，终止写入流
                }else{
                    PostRes response;
                    std::cout<<"Handle PostFeedByFollowCall, "<<num<<std::endl;
                    /*handle database*/

                    response=post_list[num-1];

                    serverAsyncWriter.Write(response,this);
                }
            }else{//FINISH状态，释放该CallData实例(一个客户端的一次rpc访问)
                GPR_ASSERT(status_==FINISH);
                std::cout<<("\033[32mcall ok\033[0m\n")<<std::endl;
                delete this;
            }
        }

        int num;
        int need=10;
        std::vector<PostRes> post_list;
        PostFeedReq request;
        grpc::ServerAsyncWriter<PostRes> serverAsyncWriter;
    };

    static void QueryPostByVisits(std::vector<PostRes>& post_list){
        //锁
        const std::lock_guard<std::mutex> lock(db_mutex);
        std::cout<<"thread "<<std::this_thread::get_id<<" QueryPostByVisits"<<std::endl;

        stmt=con->prepareStatement("SELECT * FROM post ORDER BY visits DESC LIMIT 10");
        sql::ResultSet* resultSet=stmt->executeQuery();
        while(resultSet->next()){
            PostRes tmp;
            tmp.set_text(resultSet->getString("text"));
            tmp.set_post_id(resultSet->getInt("id"));
            tmp.set_publish_time(resultSet->getString("publish_time"));
            post_list.emplace_back(tmp);
        }
    }

    static void QueryPostByFollow(std::vector<PostRes>& post_list,int user_id){
        //锁
        const std::lock_guard<std::mutex> lock(db_mutex);
        std::cout<<"thread "<<std::this_thread::get_id<<" QueryPostByFollow"<<std::endl;

        stmt=con->prepareStatement("SELECT p.id, p.author_id, u.user_name,p.text, p.publish_time\n"
                                   "FROM post AS p\n"
                                   "JOIN `user` AS u ON p.author_id = u.id\n"
                                   "JOIN fans AS f ON u.id = f.user_id\n"
                                   "WHERE f.fan_id = ? AND p.id < ?;\n"
                                   "ORDER BY p.publish_time DESC\n"
                                   "LIMIT 10;");
        stmt->setInt(1,user_id);
        stmt->setInt(2,last_query_row_id);
        sql::ResultSet* resultSet=stmt->executeQuery();
        while(resultSet->next()){
            PostRes tmp;
            tmp.set_text(resultSet->getString("text"));
            tmp.set_post_id(resultSet->getInt("id"));
            tmp.set_author_name(resultSet->getString("user_name"));
            tmp.set_publish_time(resultSet->getString("publish_time"));
            post_list.emplace_back(tmp);
        }
        last_query_row_id=post_list.back().post_id();
    }

    static void QueryPostByUserId(std::vector<PostRes>& post_list,int user_id){
        //锁
        const std::lock_guard<std::mutex> lock(db_mutex);
        std::cout<<"thread "<<std::this_thread::get_id<<" QueryPostByUserId"<<std::endl;

        stmt=con->prepareStatement("SELECT text,post.id,user_name FROM `user` JOIN post ON `user`.id=post.author_id WHERE author_id=?");
        stmt->setInt(1,user_id);
        sql::ResultSet* resultSet=stmt->executeQuery();
        while(resultSet->next()){
            PostRes tmp;
            tmp.set_text(resultSet->getString("text"));
            tmp.set_post_id(resultSet->getInt("id"));
            tmp.set_author_name(resultSet->getString("user_name"));
            tmp.set_publish_time(resultSet->getString("publish_time"));
            post_list.emplace_back(tmp);
        }
    }

    void HandelRpc(){
        //生成一个新的CallData实例来为新客户端提供服务，相当于待命
        new GetHotCall(&service_,cq_.get(),20);
        new RegistCall(&service_,cq_.get());
        new LoginCall(&service_,cq_.get());
        new GetPostByUserIdCall(&service_,cq_.get());
        new PostFeedByRecommend(&service_,cq_.get());
        new PostFeedByFollowCall(&service_,cq_.get());
        new PublishPostCall(&service_,cq_.get());

        void* tag;
        bool ok=false;
        while (true){
            GPR_ASSERT(cq_->Next(&tag,&ok));
            CallData* handle= static_cast<CallData*>(tag);
            if(!ok){
                printf("Got a canceled events, Maybe connection is closed unusually.\n");
                handle->status_ = FINISH;
            }
            handle->Proceed();
        }
    }

    //gRPC服务器
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;
    Weibo::AsyncService service_;
    std::unique_ptr<Server> server_;
    static std::vector<PostRes> common_posts_pool;

    //锁
    static std::mutex db_mutex;

    static int cpp_len;
    static int cpp_id;
    static int last_query_row_id;
};

std::mutex AsyncServerImpl::db_mutex{};
int AsyncServerImpl::cpp_id=0;
int AsyncServerImpl::cpp_len=0;
int AsyncServerImpl::last_query_row_id=10000000;

std::vector<PostRes> AsyncServerImpl::common_posts_pool= {};

int main() {
    AsyncServerImpl server;
    server.FlushCommonPostsPool();
    server.Run();
    return 0;
}
