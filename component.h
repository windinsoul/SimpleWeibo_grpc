//
// Created by windinsoul on 23-5-23.
//
#include <boost/uuid/detail/md5.hpp>
#include<string>
#include<iomanip>
#include<sstream>
#include<grpcpp/grpcpp.h>


std::string CalculateMD5(const std::string& input){
    boost::uuids::detail::md5 md5;
    boost::uuids::detail::md5::digest_type digest;
    md5.process_bytes(input.data(),input.size());
    md5.get_digest(digest);

    std::stringstream ss;
    for(int i=0;i<16;i++){
        ss<<std::hex<<std::setw(2)<<std::setfill('0')<<static_cast<int>(digest[i]);
    }
    return ss.str();
}

long long GetNowTimeStamp(){
    // 获取当前时间戳
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();

    // 将时间戳转换为整数表示
    std::chrono::system_clock::duration dur = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::seconds>(dur).count();
}
