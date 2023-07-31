//
// Created by windinsoul on 23-6-17.
//

#ifndef SIMPLEWEIBO_GRPC_MESSAGE_QUEUE_H
#define SIMPLEWEIBO_GRPC_MESSAGE_QUEUE_H

#endif //SIMPLEWEIBO_GRPC_MESSAGE_QUEUE_H
#include <stdexcept>
#include <iostream>
#include <csignal>
#include<future>
#include <boost/program_options.hpp>
#include "threadpool.h"
#include <cppkafka/cppkafka.h>
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"
using cppkafka::Consumer;
using cppkafka::Configuration;
using cppkafka::Message;
using cppkafka::TopicPartitionList;
using std::cout;
using std::endl;
using std::string;

using namespace sw::redis;
using namespace cppkafka;

namespace po = boost::program_options;

namespace MQ
{
    std::queue<std::future<std::pair<int,std::string> > > results;//存放线程异步执行后的结果
    std::mutex lock;

    //处理消费者执行任务所用线程的结果,更像是异步日志
    void AsyncThreadResultHandleForMQ(){
        uint id=0;
        while(true) {
            while (!results.empty()) {
                std::future<std::pair<int,string> > now = std::move(results.front());
                auto res=now.get();
                //执行任务失败
                if(res.first==1)
                {
                    std::clog<<"Log["<<id++<<"]: "<<"Handle msg:"<<res.second<<" successfully"<<endl;
                }else{
                    std::clog<<"Log["<<id++<<"]: "<<"Handle msg:"<<res.second<<" Error"<<endl;
                }
                results.pop();
            }
        }
    }

    void ProcessMessage(const string& message,const string& topic){
        Configuration config={
                {"metadata.broker.list","127.0.0.1:9092"}
        };

        Producer producer(config);

    //    producer.produce(MessageBuilder("mytest").partition(0).payload(message));
        producer.produce(MessageBuilder(topic).partition(0).payload(message));
        producer.flush();
    }

    bool running = true;
    void consumerThread(const string& target_topic){
//        ThreadPool pool(8);
        string brokers;
        string topic_name;
        string group_id;
        // 这段代码的作用是在程序运行过程中捕捉 SIGINT 信号（用户按下 Ctrl+C），并将 running 变量设置为 false，从而使程序可以优雅地退出。
//        signal(SIGINT, [](int) { running = false; });

        // Construct the configuration
        brokers="127.0.0.1:9092";
        Configuration config = {
                { "metadata.broker.list", brokers },
                { "group.id", 1 },
                // Disable auto commit
                { "enable.auto.commit", false }
        };

        // Create the consumer
//    Consumer consumer(config);
        auto consumer=std::make_shared<Consumer>(config);
        // 当消费者加入消费者组或者当主题的分区发生重新分配时输出被分配的新分区信息，分区分配
        consumer->set_assignment_callback([](const TopicPartitionList& partitions) {
            cout << "Got assigned: " << partitions << endl;
        });

        // 用于在消费者被撤销分区时被调用，分区回收
        consumer->set_revocation_callback([](const TopicPartitionList& partitions) {
            cout << "Got revoked: " << partitions << endl;
        });

        // 订阅的主题
//        topic_name="redis_test";
        topic_name=target_topic;
        consumer->subscribe({ topic_name });

        cout << "Consuming messages from topic " << topic_name << endl;

        // 不断消费所订阅的主题中的消息
        while (running) {
            // 获取消息
//        Message msg = consumer->poll();
            auto msg=std::make_shared<Message>(consumer->poll());
            if (*msg) {
                // If we managed to get a message
                if (msg->get_error()) {
                    // Ignore EOF notifications from rdkafka
                    if (!msg->is_eof()) {
                        cout << "[+] Received error notification: " << msg->get_error() << endl;
                    }
                }
                else {
                    // Print the key (if any)
                    if (msg->get_key()) {
                        cout << msg->get_key() << " -> ";
                    }
                    // Print the payload
                    cout << msg->get_payload() << endl;

                    std::string message=msg->get_payload();
                    // 执行消息对应的任务
                    if(message=="redis_start_test")
                    {
//                    results.push(std::async(std::launch::async,[msg,message,topic_name,consumer]{
                        results.push(std::async(std::launch::async,[msg,message,topic_name,consumer]{
                            try{//正常
                                cout<<"hhh"<<std::endl;
                                auto redis = Redis("tcp://127.0.0.1:6379");
                                redis.set("start_test", "Redis Connect Successfully");
                                std::cout<<"\033[32m"<<*redis.get("start_test")<<"\033[0m"<<std::endl;

                                cout<<"Debug "<<consumer.get()<<endl;
                                consumer->commit(*msg);
                                return std::make_pair(1,message);//{消费处理结果状态码，原消息}
                            }catch(std::bad_exception e){//手动弄的可恢复错误
                                //可恢复就在这里重试，然后再提交
                                cout<<e.what()<<endl;
                                //在此同步重试然后再提交信息，不发送到重试主题
                                consumer->commit(*msg);
                                return std::make_pair(0,message);
                            }
                            catch(...){//默认为不可恢复错误
                                cout<<"handle message's task from "<<topic_name<<" failed: "<<std::endl;
                                //发往死信队列
                                ProcessMessage(message,"dead_msg");

                                consumer->commit(*msg);
                                return std::make_pair(-1,message);
                            }
                        }));

                    }else{
                        std::cout<<"normal message"<<std::endl;
                        consumer->commit(*msg);
                    }

//                results.push(pool.enqueue([]{cout<<"hhh"<<std::endl;return 1;}));

                }
            }
        }

    }

}