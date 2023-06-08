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

