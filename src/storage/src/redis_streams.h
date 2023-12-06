#pragma once

#include "include/storage/storage.h"
#include "pika_stream_base.h"
#include "pika_stream_meta_value.h"
#include "pika_stream_types.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "src/redis.h"
#include "storage/storage.h"

namespace storage {

class RedisStreams : public Redis {
 public:
  RedisStreams(Storage* const s, const DataType& type) : Redis(s, type) {}
  ~RedisStreams() override = default;

  //===--------------------------------------------------------------------===//
  // Commands
  //===--------------------------------------------------------------------===//
  Status XADD(const Slice& key, const std::string& serialized_message, Storage::StreamAddTrimArgs& args) {
    // 1 get stream meta
    rocksdb::Status s;
    StreamMetaValue stream_meta;
    s = GetStreamMeta(stream_meta, key);
    if (s.IsNotFound() && args.no_mkstream) {
      return Status::NotFound("no_mkstream");
    } else if (s.IsNotFound()) {
      stream_meta.Init();
    } else if (!s.ok()) {
      return Status::Corruption("error from XADD, get stream meta failed: " + s.ToString());
    }

    if (stream_meta.last_id().ms == UINT64_MAX && stream_meta.last_id().seq == UINT64_MAX) {
      LOG(ERROR) << "Fatal! Sequence number overflow !";
      return Status::Corruption("Fatal! Sequence number overflow !");
    }

    // 2 append the message to storage
    s = GenerateStreamID(stream_meta, args);
    if (!s.ok()) {
      return s;
    }

    // check the serialized current id is larger than last_id
#ifdef DEBUG
    std::string serialized_last_id;
    stream_meta.last_id().SerializeTo(serialized_last_id);
    assert(field > serialized_last_id);
#endif  // DEBUG

    s = InsertStreamMessage(key, args.id, serialized_message);
    if (!s.ok()) {
      return Status::Corruption("error from XADD, insert stream message failed 1: " + s.ToString());
    }

    // 3 update stream meta
    if (stream_meta.length() == 0) {
      stream_meta.set_first_id(args.id);
    }
    stream_meta.set_entries_added(stream_meta.entries_added() + 1);
    stream_meta.set_last_id(args.id);
    stream_meta.set_length(stream_meta.length() + 1);

    // 4 trim the stream if needed
    if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
      int32_t count;
      s = TrimStream(count, stream_meta, key, args);
      if (!s.ok()) {
        return Status::Corruption("error from XADD, trim stream failed: " + s.ToString());
      }
      (void)count;
    }

    // 5 update stream meta
    s = SetStreamMeta(key, stream_meta.value());
    if (!s.ok()) {
      return s;
    }

    return Status::OK();
  }

  //===--------------------------------------------------------------------===//
  // Common Commands
  //===--------------------------------------------------------------------===//
  Status Open(const StorageOptions& storage_options, const std::string& db_path) override;
  Status CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;
  Status GetProperty(const std::string& property, uint64_t* out) override;
  Status ScanKeyNum(KeyInfo* keyinfo) override;
  Status ScanKeys(const std::string& pattern, std::vector<std::string>* keys) override;
  Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) override;

  //===--------------------------------------------------------------------===//
  // Keys Commands
  //===--------------------------------------------------------------------===//
  Status Del(const Slice& key) override;
  bool Scan(const std::string& start_key, const std::string& pattern, std::vector<std::string>* keys, int64_t* count,
            std::string* next_key) override;

  //===--------------------------------------------------------------------===//
  // Not needed for streams
  //===--------------------------------------------------------------------===//
  Status Expire(const Slice& key, int32_t ttl) override;
  bool PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                    std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) override;
  Status Expireat(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;

  //===--------------------------------------------------------------------===//
  // Storage API
  //===--------------------------------------------------------------------===//
  // get and parse the stream meta if found
  // @return ok only when the stream meta exists
  storage::Status GetStreamMeta(StreamMetaValue& tream_meta, const rocksdb::Slice& key);

  // will create stream meta hash if it dosent't exist.
  // return !s.ok() only when insert failed
  storage::Status SetStreamMeta(const rocksdb::Slice& key, std::string& meta_value);

  storage::Status InsertStreamMessage(const rocksdb::Slice& key, const streamID& id, const std::string& message);

  storage::Status DeleteStreamMessage(const rocksdb::Slice& key, const std::vector<streamID>& ids);

  storage::Status DeleteStreamMessage(const rocksdb::Slice& key, const std::vector<std::string>& serialized_ids);

  storage::Status GetStreamMessage(const rocksdb::Slice& key, const std::string& sid, std::string& message);

  storage::Status DeleteStreamData(const rocksdb::Slice& key);

  storage::Status TrimStream(int32_t& count, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                             Storage::StreamAddTrimArgs& args);
  struct ScanStreamOptions {
    const rocksdb::Slice key;  // the key of the stream
    streamID start_sid;
    streamID end_sid;
    int32_t count;
    bool start_ex;    // exclude first message
    bool end_ex;      // exclude last message
    bool is_reverse;  // scan in reverse order
    ScanStreamOptions(const rocksdb::Slice skey, streamID start_sid, streamID end_sid, int32_t count,
                      bool start_ex = false, bool end_ex = false, bool is_reverse = false)
        : key(skey),
          start_sid(start_sid),
          end_sid(end_sid),
          count(count),
          start_ex(start_ex),
          end_ex(end_ex),
          is_reverse(is_reverse) {}
  };

  storage::Status ScanStream(const ScanStreamOptions& option, std::vector<storage::FieldValue>& field_values,
                             std::string& next_field);

 private:
  Status GenerateStreamID(const StreamMetaValue& stream_meta, Storage::StreamAddTrimArgs& args);

  Status ScanRange(const Slice& key, const Slice& field_start, const std::string& field_end, const Slice& pattern,
                   int32_t limit, std::vector<FieldValue>* field_values, std::string* next_field);
  Status ReScanRange(const Slice& key, const Slice& id_start, const std::string& id_end, const Slice& pattern,
                     int32_t limit, std::vector<FieldValue>* id_values, std::string* next_id);

  struct TrimRet {
    // the count of deleted messages
    int32_t count{0};
    // the next field after trim
    std::string next_field;
    // the max deleted field, will be empty if no message is deleted
    std::string max_deleted_field;
  };

  storage::Status TrimByMaxlen(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                               const Storage::StreamAddTrimArgs& args);

  storage::Status TrimByMinid(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                              const Storage::StreamAddTrimArgs& args);
};
}  // namespace storage