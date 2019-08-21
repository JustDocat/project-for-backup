#include <unordered_map>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <thread>
#include <pthread.h>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <zlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>


#define UNGZIPFILE_PATH "./www/list/"
#define GZIPFILE_PATH "www/zip/"
#define RECORD_FILE "record.list"
#define HEAT_TIME 10

namespace bf = boost::filesystem;

class CompressStore {
  private:
    std::string _file_dir;
    // 保存文件列表
    std::unordered_map<std::string, std::string> _file_list;
    pthread_rwlock_t _rwlock;
    
  private:
    // 1. 每次压缩线程启动的时候，从文件中读取列表信息
    bool GetListRecord() {
      // filename gzipfilename\n
      bf::path name(RECORD_FILE);
      if(!bf::exists(name)) {
        std::cerr << "record file is not exists" << std::endl;
        return false;
      }
      std::ifstream file(RECORD_FILE,std::ios::binary);
      if(!file.is_open()) {
        std::cerr << "open record file error" << std::endl;
        return false;
      }
      int64_t fsize = bf::file_size(name);
      std::string body;
      body.resize(fsize);
      file.read(&body[0], fsize);
      if(!file.good()) {
        std::cerr << "record file body read error" << std::endl;
        return false;
      }
      file.close();

      std::vector<std::string> list;
      boost::split(list, body, boost::is_any_of("\n"));
      for(auto& e : list) {
        // filename gzipname
        size_t pos = e.find(" ");
        if(pos == std::string::npos) {
          continue;
        }
        std::string key = e.substr(0, pos);
        std::string val = e.substr(pos + 1);
        _file_list[key] = val;
      }
      return true;
    }
    // 2. 每次压缩存储完毕，都要将列表信息，存储到文件中
    bool SetListRecord() {
      std::stringstream tmp;
      for(auto& e : _file_list) {
        tmp << e.first << " " << e.second << "\n";
      }

      std::ofstream file(RECORD_FILE, std::ios::binary|std::ios::trunc);
      if(!file.is_open()) {
        std::cerr << "record file open error" << std::endl;
        return false;
      }
      file.write(tmp.str().c_str(), tmp.str().size());
      if(!file.good()) {
        std::cerr << "recode file write body error" << std::endl;
        return false;
      }
      file.close();
      return true;
    }

    // 2.2. 判断文件是否需要压缩存储
    bool IsNeedCompress(std::string& file) {
      struct stat st;
      if(stat(file.c_str(), &st) < 0) {
        std::cerr << "get file:[" << file << "] stat error" << std::endl;
        return false;
      }
      time_t cur_time = time(NULL);
      time_t acc_time = st.st_atime;
      if((cur_time - acc_time) < HEAT_TIME) {
        return false;
      }
      return true;
    }
    // 2.3. 对文件进行压缩存储
    bool CompressFile(std::string& file, std::string& gzip) {
      int fd = open(file.c_str(), O_RDONLY);
      if(fd < 0) {
        std::cerr << "com open file:[" << file << "] error" << std::endl;
        return false;
      }
      gzFile gf = gzopen(gzip.c_str(), "wb");
      if(gf == NULL) {
        std::cerr << "com open gzip:[" << gzip << "] error" << std::endl;
        return false;
      }
      int ret;
      char buf[1024];
      flock(fd, LOCK_SH);
      while((ret = read(fd, buf, 1024)) > 0) {
        gzwrite(gf, buf, ret);
      }
      flock(fd, LOCK_UN);
      close(fd);
      gzclose(gf);
      unlink(file.c_str());
      // 文件正在被系统或其他进程使用时无法删除
      return true;
    }
    // 3. 对文件进行解压缩
    bool UnCompressFile(std::string& gzip, std::string& file) {
      int fd = open(file.c_str(), O_CREAT|O_WRONLY, 0664);
      if(fd < 0) {
        std::cerr << "open file " << file << "failed" << std::endl;
        return false;
      }
      gzFile gf = gzopen(gzip.c_str(), "rb");
      if(gf == NULL) {
        std::cerr << "open gzip " << gzip << " failed" << std::endl;
        close(fd);
        return false;
      }
      int ret;
      char buf[1024];
      flock(fd, LOCK_EX);
      while((ret = gzread(gf, buf, 1024)) > 0) {
        int len = write(fd, buf, ret);
        if(len < 0) {
          std::cerr << "get gzip data failed" << std::endl;
          gzclose(gf);
          close(fd);
          flock(fd, LOCK_UN);
          return false;
        }
      }
      flock(fd, LOCK_UN);
      gzclose(gf);
      close(fd);
      unlink(gzip.c_str());
      return true;
    }
    bool GetNormalFile(std::string& name, std::string& body) {
      int64_t fsize = bf::file_size(name);
      body.resize(fsize);

      int fd = open(name.c_str(), O_RDONLY);
      if(fd < 0) {
        std::cerr << "open file " << name << " failed" << std::endl;
        return false;
      }

      flock(fd, LOCK_SH);
      int ret = read(fd, &body[0], fsize);
      flock(fd, LOCK_UN);

      if(ret != fsize) {
        std::cerr << "get file " << name << " body error" << std::endl;
        close(fd);
        return false;
      }
      close(fd);
      return true;
    }

    // 目录检测，获取目录中的文件名
    // 1. 判断文件是否需要压缩存储
    // 2. 文件压缩存储
    bool DirectoryCheck() {
      if(!bf::exists(UNGZIPFILE_PATH)) {
        bf::create_directory(UNGZIPFILE_PATH);
      }
      bf::directory_iterator item_begin(UNGZIPFILE_PATH);
      bf::directory_iterator item_end;
      for(; item_begin != item_end; ++item_begin) {
        if(bf::is_directory(item_begin->status())) {
          continue;
        }
        std::string name = item_begin->path().string();

        if(IsNeedCompress(name)) {
          std::string gzip = GZIPFILE_PATH +  item_begin->path().filename().string() + ".gz";
          CompressFile(name, gzip);
          AddFileRecord(name, gzip);
        }
      }
      return true;
    }
    // 通过文件名称，获取文件对应的压缩包名称
    bool GetFileGzip(std::string& file, std::string& gzip) {
      std::cerr <<"file - gzip: " <<file << _file_list[file] <<  std::endl;
      pthread_rwlock_rdlock(&_rwlock);

      auto it = _file_list.find(file);
      if(it == _file_list.end()) {
        pthread_rwlock_unlock(&_rwlock);
        return false;
      }
      gzip = it->second;

      pthread_rwlock_unlock(&_rwlock);
      return true;
    }
    bool AddFileRecord(const std::string& file, const std::string& gzip) {
      pthread_rwlock_wrlock(&_rwlock);
      _file_list[file] = gzip; 
      pthread_rwlock_unlock(&_rwlock);
      std::cerr << file << "-" << gzip << std::endl;
      return true;
    }

  public:
    CompressStore() {
      pthread_rwlock_init(&_rwlock, NULL);
      if(!bf::exists(GZIPFILE_PATH)) {
        bf::create_directory(GZIPFILE_PATH);
      }
    }
    ~CompressStore() {
      pthread_rwlock_destroy(&_rwlock);
    }
    // 向外提供获取文件列表功能
    bool GetFileList(std::vector<std::string>& list) {
      pthread_rwlock_rdlock(&_rwlock);
      for(auto& e : _file_list) {
        list.push_back(e.first);
      }
      pthread_rwlock_unlock(&_rwlock);
      return true;
    }
    
    // 向外提供文件获取的功能
    bool GetFileData(std::string& file, std::string& body) {
      if(bf::exists(file)) {
        // 1. 非压缩文件获取
        GetNormalFile(file, body);
      }else {
        // 2. 压缩文件获取
        // 获取压缩包名称 gzip

        std::cerr << "i find file: "<< file << std::endl;
        std::string gzip;
        GetFileGzip(file, gzip);
        std::cerr << "GetFileGzip: gzip: " << gzip << std::endl;
        UnCompressFile(gzip, file);
        GetNormalFile(file, body);
      }
      return true;
    }
    bool SetFileData(const std::string& file, const std::string& body, const int64_t offset) {
      int fd = open(file.c_str(), O_CREAT|O_WRONLY, 0664);
      if(fd < 0) {
        std::cerr << "open file " << file << " error" << std::endl;
        return false;
      }
      flock(fd, LOCK_EX);
      lseek(fd, offset, SEEK_SET);
      int ret = write(fd, &body[0], body.size());
      if(ret < 0) {
        std::cerr << "store file" << file << " data error" << std::endl;
        flock(fd, LOCK_UN);
        return false;
      }
      flock(fd, LOCK_UN);
      close(fd);
      AddFileRecord(file, "");
      return true;
    }
    
    // 对热度低的文件进行压缩存储
    // 因为压缩存储时流程是死循环，因此需要启动线程
    bool LowHeatFileStore() {
      // 1. 获取记录信息
      GetListRecord();
      while(1) {
        // 2. 目录检测，文件压缩存储
          // 2.1 获取list目录下文件名称
          // 2.2 判断文件是否需要压缩存储
          // 2.3 对文件进行压缩存储
        DirectoryCheck();
        // 3. 存储记录信息 
        SetListRecord();
        sleep(3);

      }
      return true;
    }
};
