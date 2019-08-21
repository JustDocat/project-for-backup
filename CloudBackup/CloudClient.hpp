#ifndef __M_CLOUD_H__
#define __M_CLOUD_H__

#include <iostream>
#include <fstream>
#include <sstream>

#include <unordered_map>
#include <string>
#include <vector>
#include <thread> 
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

#include "httplib.h"

#define CLIENT_BACKUP_DIR "backup"
#define CLIENT_BACKUP_INFO_FILE "back.list"
#define RANGE_MAX_SIZE (10 << 20)
#define SERVER_IP "192.168.48.132"
#define SERVER_PORT 9000
#define BACKUP_URI "/list/"

namespace bf = boost::filesystem;

class ThrBackUp {
private:
	std::string _file;
	int64_t _range_start;
	int64_t _range_len;
public:
	bool _res;
public:
	ThrBackUp(const std::string& file, int64_t start, int64_t len) 
		: _res(true)
		, _file(file)
		, _range_start(start)
		, _range_len(len)
	{}
	void Start() {
		// 获取文件的range分块

		std::ifstream path(_file, std::ios::binary);
		if (!path.is_open()) {
			std::cerr << "range backup file " << _file << std::endl;
			_res = false;
			return;
		}
		// 跳转到range的起始位置
		path.seekg(_range_start, std::ios::beg);
		std::string body;
		body.resize(_range_len);
		// 读取文件中的range分块的数据
		path.read(&body[0], _range_len);
		if (!path.good()) {
			std::cerr << "read file " << _file << " range data failed" << std::endl;
			_res = false;
			return;
		}
		path.close();

		std::cerr << "read over" << std::endl;

		// 上传range数据
		bf::path name(_file);
		// 组织上传的url路径 method url version
		// PUT /list/filename HTTP/1.1
		std::string url = BACKUP_URI + name.filename().string();
		// 实例化一个httplib的客户端对象
		httplib::Client cli(SERVER_IP, SERVER_PORT);
		// 定义http请求的头信息
		httplib::Headers hdr;
		hdr.insert(std::make_pair("Content-Length", std::to_string(_range_len)));
		std::stringstream tmp;
		tmp << "bytes=" << _range_start << "-" << (_range_start + _range_len - 1);
		hdr.insert(std::make_pair("Range", tmp.str().c_str()));
		// 通过实例化的client向服务端发送PUT请求
		auto rsp = cli.Put(url.c_str(), hdr, body, "text/plain");
		if (rsp && rsp->status == 200) {
			std::stringstream ss;
			ss << "backup file [" << _file << "] range:[" << _range_start << "-" << _range_len << "] backup success" << std::endl;
			std::cout << ss.str();
		}
		else {	
			std::cerr << "rsp put error" << std::endl;
			_res = false;
		}
		
	}

};

class CloudClient {
private:
	std::unordered_map<std::string, std::string> _backup_list;
public:
	bool Start() {
		GetBackupInfo();
		while (1)
		{
			BackupDirListen(CLIENT_BACKUP_DIR);
			SetBackupInfo();
			Sleep(1000);
		}
		return true;
	}

	CloudClient() {
		bf::path file(CLIENT_BACKUP_DIR);
		if (!bf::exists(file))
		{
			bf::create_directories(file);
		}
	}
private:
	bool GetBackupInfo() {
		// filename1 etag\n
		// filename2 etag\n

		bf::path path(CLIENT_BACKUP_INFO_FILE);
		if (!bf::exists(path)) {
			std::cerr << "list file " << path.string() << " is not exists" << std::endl;
			return false;
		}
		int64_t fsize = bf::file_size(path);
		if (fsize == 0) {
			std::cerr << "have no backup info" << std::endl;
			return false;
		}

		std::string body;
		body.resize(fsize);
		std::ifstream file(CLIENT_BACKUP_INFO_FILE, std::ios::binary);
		if (!file.is_open()) {
			std::cerr << "list file open error" << std::endl;
			return false;
		}
		file.read(&body[0], fsize);
		if (!file.good()) {
			std::cerr << "read list file body error" << std::endl;
			return false;
		}
		file.close();

		std::vector<std::string> list;
		boost::split(list, body, boost::is_any_of("\n"));
		for (auto& e : list)
		{
			size_t pos = e.find(" ");
			if (pos == std::string::npos) {
				continue;
			}
			std::string key = e.substr(0, pos);
			std::string val = e.substr(pos + 1);
			_backup_list[key] = val;
		}
		return true;
	}
	bool SetBackupInfo() {
		std::string body;
		for (auto& e : _backup_list)
		{
			body += e.first + " " + e.second + "\n";
		}
		std::ofstream file(CLIENT_BACKUP_INFO_FILE, std::ios::binary);
		if (!file.is_open()) {
			std::cerr << "open list file error" << std::endl;
			return false;
		}
		file.write(&body[0], body.size());
		if (!file.good()) {
			std::cerr << "set backup info error" << std::endl;
			return false;
		}
		return true;
	}
	bool BackupDirListen(const std::string& path) {
		bf::path file(path);

		bf::directory_iterator item_begin(file);
		bf::directory_iterator item_end;
		for (; item_begin != item_end; ++item_begin) {
			if (bf::is_directory(item_begin->status())) {
				BackupDirListen(item_begin->path().string());
				continue;
			}
			if (FileIsNeedBackup(item_begin->path().string()) == false) {
				continue;
			}
			std::cerr << "file:[" << item_begin->path().string() << " need backup" << std::endl;
			if (PutFileData(item_begin->path().string()) == false) {
				std::cerr << "PutFileData false" << std::endl;
				continue;
			}
			AddBackInfo(item_begin->path().string());
		}
		return true;
	}
	bool AddBackInfo(const std::string& file) {
		// etag = "mtime-fsize"
		std::string etag;
		if (GetFileEtag(file, etag) == false) {
			return false;
		}

		_backup_list[file] = etag;
		return true;
	}
	bool GetFileEtag(const std::string& file, std::string& etag) {
		bf::path path(file);
		if (!bf::exists(path)) {
			std::cerr << "get file " << file << " etag error" << std::endl;
			return false;
		}
		int64_t fsize = bf::file_size(path);
		int64_t mtime = bf::last_write_time(path);

		std::stringstream tmp;
		tmp << std::hex << fsize << "-" << std::hex << mtime;
		etag = tmp.str();
		// std::cout << "GetEtag information : " << tmp.str() << std::endl;
		return true;
	}

	
	bool FileIsNeedBackup(const std::string& file) {
		std::string etag;
		if (GetFileEtag(file, etag) == false) {
			return false;
		}
		auto it = _backup_list.find(file);

		if (it != _backup_list.end() && it->second == etag) {
			return false;
		}
		return true;
	}

	static void thr_start(ThrBackUp* backup_info) {
		std::cerr << "into thread" << std::endl;
		backup_info->Start();
	}
	bool PutFileData(const std::string& file) {
		// 按分块大小 RANGE_MAX_SIZE(10M) 对文件进行分块传输
		// 通过获取分块传输是否成功判断整个文件是否上传成功
		// 选择多线程处理

		// 1. 获取文件大小
		std::cerr << "PutFileData()" << std::endl;
		int64_t fsize = bf::file_size(file);
		if (fsize <= 0) {
			std::cerr << "file " << file << "unnecessary backup" << std::endl;
		}
		// 2. 计算总共需要分多少块，得到每块大小以及起始位置
		// 3. 循环创建线程，在线程中上传文件数据
		int count = (int)(fsize / RANGE_MAX_SIZE);
		// 文件大小小于RANGE_MAX_SIZE
		std::vector<ThrBackUp> thr_res;
		std::vector<std::thread> thr_list;
		std::cerr << "file:[" << file << "] fsize:[" << fsize << "] count:[" << count + 1 << "]" << std::endl;
		for (int i = 0; i <= count; ++i)
		{
			int64_t range_start = i * RANGE_MAX_SIZE;
			int64_t range_end = (i + 1) * RANGE_MAX_SIZE - 1;
			if (i == count) {
				range_end = fsize - 1;
			}
			int64_t range_len = (range_end - range_start + 1);
			
			ThrBackUp backup_info(file, range_start, range_len);
			thr_res.push_back(backup_info);
			
		}
		for (int i = 0; i <= count; ++i) {
			thr_list.push_back(std::thread(thr_start, &thr_res[i]));
		}

		// 4. 等待所有线程退出，判断文件上传结果
		bool ret = true;
		for (int i = 0; i <= count; ++i) {
			thr_list[i].join();
			if (thr_res[i]._res == true) {
				continue;
			}
			ret = false;
		}
		// 5. 上传成功，则添加文件的备份信息记录
		if (ret == false) {
			return false;
		}
		std::cerr << "file:[" << file << "] backup success" << std::endl;
		return true;
	}
	
};

#endif