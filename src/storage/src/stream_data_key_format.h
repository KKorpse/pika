#pragma once

#include <memory.h>
#include "pstd/include/pstd_coding.h"
#include "storage/storage.h"

namespace storage {

class StreamDataKey {
 public:
  StreamDataKey(const Slice& key, const Slice& data) : key_(key), message_id_(data) {}

  ~StreamDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice Encode() {
    size_t needed = key_.size() + sizeof(int32_t) + message_id_.size();
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];

      // Need to allocate space, delete previous space
      if (start_ != space_) {
        delete[] start_;
      }
    }

    start_ = dst;
    pstd::EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    memcpy(dst, message_id_.data(), message_id_.size());
    return Slice(start_, needed);
  }

 private:
  char space_[200];
  char* start_ = nullptr;
  Slice key_;
  Slice message_id_;
};

class ParsedStreamDataKey {
 public:
  explicit ParsedStreamDataKey(const std::string* key) {
    const char* ptr = key->data();
    auto key_len = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    message_id_ = Slice(ptr, key->size() - key_len - sizeof(int32_t) * 2);
  }

  explicit ParsedStreamDataKey(const Slice& key) {
    const char* ptr = key.data();
    auto key_len = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    message_id_ = Slice(ptr, key.size() - key_len - sizeof(int32_t) * 2);
  }

  virtual ~ParsedStreamDataKey() = default;

  Slice key() { return key_; }

  Slice data() { return message_id_; }

 protected:
  Slice key_;
  Slice message_id_;
};

}  // namespace storage