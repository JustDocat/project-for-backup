#include <boost/filesystem.hpp>
#include "httplib.h"
#include "compress.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <fstream>

#define SERVER_BASE_DIR "./www"
#define SERVER_ADDR "0.0.0.0"
#define SERVER_PORT 9000
#define SERVER_BACKUP_DIR "./www/list"


using namespace httplib;
namespace bf = boost::filesystem;

CompressStore cstor;

class CloudServer {
  private:
    httplib::Server srv;
  public:
    CloudServer(){
      bf::path base_path(SERVER_BASE_DIR);
      if(!bf::exists(base_path))
      {
        bf::create_directory(base_path);
      }
      bf::path list_path(SERVER_BACKUP_DIR);
      if(!bf::exists(SERVER_BACKUP_DIR))
      {
        bf::create_directory(SERVER_BACKUP_DIR);
      }
    }
    bool Start()
    {
      srv.set_base_dir(SERVER_BACKUP_DIR);
      srv.Get("/(list(/){0,1}){0,1}", GetFileList);
      srv.Get("/list/(.*)", GetFileData);
      srv.Put("/list/(.*)", PutFileData);
      srv.listen(SERVER_ADDR, SERVER_PORT);
      return true;
    }
  private:
    
    static void PutFileData(const Request &req, Response &rsp){
      std::cerr<< "backup file " << req.path << std::endl;
      if(!req.has_header("Range")) {
        rsp.status = 400;
        return;
      }
      std::string range = req.get_header_value("Range");
      std::cout << range << std::endl;
      int64_t range_start;

      if(RangeParse(range, range_start)== false) {
        rsp.status = 400;
        std::cerr << "RangeParse() error" << std::endl;
        return;
      }
      std::string realpath = SERVER_BASE_DIR + req.path;
      cstor.SetFileData(realpath, req.body, range_start);
    }

    // 分块，多线程传输
    static bool RangeParse(std::string& range, int64_t &start) {
      // Range: bytes=start-end
      size_t pos1 = range.find("=");
      size_t pos2 = range.find("-");
      std::cout << pos1 << "-" << pos2 << std::endl;
      if(pos1 == std::string::npos || pos2 == std::string::npos) {
        std::cerr << "range:[" << range << "] format error";
        return false;
      }
      std::stringstream rs;
      rs << range.substr(pos1 + 1, pos2 - pos1 - 1);
      rs >> start;
      return true;
    }

    // 获取文件列表
    static void GetFileList(const Request &req, Response &rsp){

      std::vector<std::string> list;
      cstor.GetFileList(list);
      std::string body;
      body += "<html><body><ol><hr />";
      for(auto& e : list)
      {
        bf::path path(e);
        
        std::string file = path.filename().string();
        std::string uri = "/list/" + file;

        body += "<h4><li>";

        body += "<a href = '";
        body += uri;
        body += "'>";

        body += file; 
        body += "</a>";
        body += "</li></h4>";

        // "<h4><li><a href = '/list/filename'>filename</a></li></h4>"
      }
      body += "<hr /></ol></body></html>";
      rsp.set_content(&body[0],"text/html");
      return;
    }

    // 下载(数据获取)
    static void GetFileData(const Request &req, Response &rsp){
      std::string realpath = SERVER_BASE_DIR + req.path;
      std::string body;

      cstor.GetFileData(realpath, body);
      rsp.set_content(body, "text/plain");
    }
};

void thr_start() {
  cstor.LowHeatFileStore();
}

int main()
{
  std::thread thr(thr_start);
  thr.detach();
  CloudServer srv;
  srv.Start();
  return 0;
}

