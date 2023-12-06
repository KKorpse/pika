#include "src/redis_streams.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include "pika_stream_base.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "src/base_filter.h"
#include "src/debug.h"
#include "src/pika_stream_meta_value.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "storage/storage.h"
#include "storage/util.h"
#include "stream_data_key_format.h"

namespace storage {

Status RedisStreams::XADD(const Slice& key, const std::string& serialized_message, StreamAddTrimArgs& args) {
  ScopeRecordLock l(lock_mgr_, key);

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = GetStreamMeta(stream_meta, key, read_options);
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
    s = TrimStream(count, stream_meta, key, args, read_options);
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

Status RedisStreams::XDEL(const Slice& key, const std::vector<streamID>& ids, size_t& count) {
  assert(count == 0);
  ScopeRecordLock l(lock_mgr_, key);

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  // 1 try to get stream meta
  StreamMetaValue stream_meta;
  auto s = GetStreamMeta(stream_meta, key, read_options);
  if (!s.ok()) {
    return s;
  }

  // 2 do the delete
  s = DeleteStreamMessage(key, ids, count, read_options);
  if (!s.ok()) {
    return s;
  }

  // 3 update stream meta
  stream_meta.set_length(stream_meta.length() - count);
  for (const auto& id : ids) {
    if (id > stream_meta.max_deleted_entry_id()) {
      stream_meta.set_max_deleted_entry_id(id);
    }
    if (id == stream_meta.first_id()) {
      s = SetFirstID(key, stream_meta, read_options);
    } else if (id == stream_meta.last_id()) {
      s = SetLastID(key, stream_meta, read_options);
    }
    if (!s.ok()) {
      return s;
    }
  }

  return SetStreamMeta(key, stream_meta.value());
}

Status RedisStreams::Open(const StorageOptions& storage_options, const std::string& db_path) {
  statistics_store_->SetCapacity(storage_options.statistics_max_size);
  small_compaction_threshold_ = storage_options.small_compaction_threshold;

  rocksdb::Options ops(storage_options.options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // create column family
    rocksdb::ColumnFamilyHandle* cf;
    // FIXME: Dose stream data need a comparater ?
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete cf;
    delete db_;
  }

  // Open
  rocksdb::DBOptions db_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions data_cf_ops(storage_options.options);
  // Notice: Stream's Meta dose not have timestamp and version, so it does not need to be filtered.

  // use the bloom filter policy to reduce disk reads
  rocksdb::BlockBasedTableOptions table_ops(storage_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  rocksdb::BlockBasedTableOptions meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(meta_cf_table_ops));
  data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(data_cf_table_ops));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, meta_cf_ops);
  // Data CF
  column_families.emplace_back("data_cf", data_cf_ops);
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisStreams::CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end,
                                  const ColumnFamilyType& type) {
  if (type == kMeta || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[0], begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[1], begin, end);
  }
  return Status::OK();
}

Status RedisStreams::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), nullptr, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), nullptr, 10);
  return Status::OK();
}

// Check if the key has prefix of STERAM_TREE_PREFIX.
// That means the key-value is a virtual tree node, not a stream meta.
bool IsVirtualTree(rocksdb::Slice key) {
  if (key.size() < STERAM_TREE_PREFIX.size()) {
    return false;
  }

  if (memcmp(key.data(), STERAM_TREE_PREFIX.data(), STERAM_TREE_PREFIX.size()) == 0) {
    return true;
  }

  return false;
}

Status RedisStreams::ScanKeyNum(KeyInfo* key_info) {
  uint64_t keys = 0;
  uint64_t expires = 0;
  uint64_t ttl_sum = 0;
  uint64_t invaild_keys = 0;

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  int64_t curtime;
  rocksdb::Env::Default()->GetCurrentTime(&curtime);

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (IsVirtualTree(iter->key())) {
      continue;
    }
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(iter->value());
    if (stream_meta_value.length() == 0) {
      invaild_keys++;
    } else {
      keys++;
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->invaild_keys = invaild_keys;
  return Status::OK();
}

Status RedisStreams::ScanKeys(const std::string& pattern, std::vector<std::string>* keys) {
  std::string key;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (IsVirtualTree(iter->key())) {
      continue;
    }
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(iter->value());
    if (stream_meta_value.length() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisStreams::PKPatternMatchDel(const std::string& pattern, int32_t* ret) {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  std::string key;
  std::string meta_value;
  int32_t total_delete = 0;
  Status s;
  rocksdb::WriteBatch batch;
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  iter->SeekToFirst();
  while (iter->Valid()) {
    if (IsVirtualTree(iter->key())) {
      iter->Next();
      continue;
    }
    key = iter->key().ToString();
    meta_value = iter->value().ToString();
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(iter->value());
    if ((stream_meta_value.length() != 0) &&
        (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0)) {
      stream_meta_value.Reset();
      batch.Put(handles_[0], key, meta_value);
    }
    if (static_cast<size_t>(batch.Count()) >= BATCH_DELETE_LIMIT) {
      s = db_->Write(default_write_options_, &batch);
      if (s.ok()) {
        total_delete += static_cast<int32_t>(batch.Count());
        batch.Clear();
      } else {
        *ret = total_delete;
        return s;
      }
    }
    iter->Next();
  }
  if (batch.Count() != 0U) {
    s = db_->Write(default_write_options_, &batch);
    if (s.ok()) {
      total_delete += static_cast<int32_t>(batch.Count());
      batch.Clear();
    }
  }

  *ret = total_delete;
  return s;
}

Status RedisStreams::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(meta_value);
    if (stream_meta_value.length() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = stream_meta_value.length();
      stream_meta_value.Reset();
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
      UpdateSpecificKeyStatistics(key.ToString(), static_cast<size_t>(statistic));
    }
  }
  return s;
}

bool RedisStreams::Scan(const std::string& start_key, const std::string& pattern, std::vector<std::string>* keys,
                        int64_t* count, std::string* next_key) {
  std::string meta_key;
  bool is_finish = true;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    if (IsVirtualTree(it->key())) {
      it->Next();
      continue;
    }
    StreamMetaValue stream_meta_value;
    stream_meta_value.ParseFrom(it->value());
    if (stream_meta_value.length() == 0) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), meta_key.data(), meta_key.size(), 0) != 0) {
        keys->push_back(meta_key);
      }
      (*count)--;
      it->Next();
    }
  }

  std::string prefix = isTailWildcard(pattern) ? pattern.substr(0, pattern.size() - 1) : "";
  if (it->Valid() && (it->key().compare(prefix) <= 0 || it->key().starts_with(prefix))) {
    *next_key = it->key().ToString();
    is_finish = false;
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisStreams::Expire(const Slice& key, int32_t ttl) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Expire not supported by stream"));
  return Status::Corruption(s.ToString());
}

bool RedisStreams::PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                                std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) {
  TRACE(FATAL, "RedisStreams::PKExpireScan not supported by stream");
  return false;
}

Status RedisStreams::Expireat(const Slice& key, int32_t timestamp) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Expireat not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::Persist(const Slice& key) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Persist not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::TTL(const Slice& key, int64_t* timestamp) {
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::TTL not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::GetStreamMeta(StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                   rocksdb::ReadOptions& read_options) {
  std::string value;
  auto s = db_->Get(read_options, handles_[0], key, &value);
  if (s.ok()) {
    stream_meta.ParseFrom(value);
    return Status::OK();
  }
  return s;
}

Status RedisStreams::SetStreamMeta(const rocksdb::Slice& key, std::string& meta_value) {
  return db_->Put(default_write_options_, handles_[0], key, meta_value);
}

Status RedisStreams::InsertStreamMessage(const rocksdb::Slice& key, const streamID& id, const std::string& message) {
  return db_->Put(default_write_options_, handles_[1], key, message);
}

Status RedisStreams::GetStreamMessage(const rocksdb::Slice& key, const std::string& sid, std::string& message,
                                      rocksdb::ReadOptions& read_options) {
  return db_->Get(read_options, handles_[1], key, &message);
}

Status RedisStreams::DeleteStreamMessage(const rocksdb::Slice& key, const std::vector<streamID>& ids, size_t& count,
                                         rocksdb::ReadOptions& read_options) {
  assert(count == 0);
  Status s;
  std::string data_value;
  rocksdb::WriteBatch batch;
  for (const auto& id : ids) {
    StreamDataKey stream_data_key(key, id.ToString());
    s = db_->Get(read_options, handles_[1], stream_data_key.Encode(), &data_value);
    if (s.ok()) {
      count++;
      batch.Delete(handles_[1], stream_data_key.Encode());
    } else if (s.IsNotFound()) {
      continue;
    } else {
      return s;
    }
  }

  return db_->Write(default_write_options_, &batch);
}

Status RedisStreams::DeleteStreamMessage(const rocksdb::Slice& key, const std::vector<std::string>& serialized_ids) {
  rocksdb::WriteBatch batch;
  for (auto& sid : serialized_ids) {
    batch.Delete(handles_[1], sid);
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisStreams::TrimStream(int32_t& count, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options) {
  count = 0;
  // 1 do the trim
  TrimRet trim_ret;
  Status s;
  if (args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MAXLEN) {
    s = TrimByMaxlen(trim_ret, stream_meta, key, args, read_options);
  } else {
    assert(args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MINID);
    s = TrimByMinid(trim_ret, stream_meta, key, args, read_options);
  }

  if (!s.ok()) {
    return s;
  }

  if (trim_ret.count == 0) {
    return s;
  }

  // 2 update stream meta
  streamID first_id;
  streamID max_deleted_id;
  if (stream_meta.length() == trim_ret.count) {
    // all the message in the stream were deleted
    first_id = kSTREAMID_MIN;
  } else {
    first_id.DeserializeFrom(trim_ret.next_field);
  }
  assert(!trim_ret.max_deleted_field.empty());
  max_deleted_id.DeserializeFrom(trim_ret.max_deleted_field);

  stream_meta.set_first_id(first_id);
  if (max_deleted_id > stream_meta.max_deleted_entry_id()) {
    stream_meta.set_max_deleted_entry_id(max_deleted_id);
  }
  stream_meta.set_length(stream_meta.length() - trim_ret.count);

  count = trim_ret.count;

  return Status::OK();
}

Status RedisStreams::ScanStream(const ScanStreamOptions& op, std::vector<FieldValue>& field_values,
                                std::string& next_field, rocksdb::ReadOptions& read_options) {
  std::string start_field;
  std::string end_field;
  Slice pattern = "*";  // match all the fields from start_field to end_field
  Status s;

  // 1 do the scan
  if (op.is_reverse) {
    op.end_sid.SerializeTo(start_field);
    if (op.start_sid == kSTREAMID_MAX) {
      start_field = "";
    } else {
      op.start_sid.SerializeTo(start_field);
    }
    s = ScanRange(op.key, start_field, end_field, pattern, op.count, &field_values, &next_field, read_options);
  } else {
    op.start_sid.SerializeTo(start_field);
    if (op.end_sid == kSTREAMID_MAX) {
      end_field = "";
    } else {
      op.end_sid.SerializeTo(end_field);
    }
    s = ReScanRange(op.key, start_field, end_field, pattern, op.count, &field_values, &next_field, read_options);
  }

  // 2 exclude the start_sid and end_sid if needed
  if (op.start_ex && !field_values.empty()) {
    streamID sid;
    sid.DeserializeFrom(field_values.front().field);
    if (sid == op.start_sid) {
      field_values.erase(field_values.begin());
    }
  }

  if (op.end_ex && !field_values.empty()) {
    streamID sid;
    sid.DeserializeFrom(field_values.back().field);
    if (sid == op.end_sid) {
      field_values.pop_back();
    }
  }

  return s;
}

// FIXME: CHECK
Status RedisStreams::DeleteStreamData(const rocksdb::Slice& key) {
  return db_->Delete(default_write_options_, handles_[1], key);
}

Status RedisStreams::GenerateStreamID(const StreamMetaValue& stream_meta, StreamAddTrimArgs& args) {
  auto& id = args.id;
  if (args.id_given && args.seq_given && id.ms == 0 && id.seq == 0) {
    return Status::InvalidArgument("The ID specified in XADD must be greater than 0-0");
  }

  if (!args.id_given || !args.seq_given) {
    // if id not given, generate one
    if (!args.id_given) {
      id.ms = StreamUtils::GetCurrentTimeMs();

      if (id.ms < stream_meta.last_id().ms) {
        id.ms = stream_meta.last_id().ms;
        if (stream_meta.last_id().seq == UINT64_MAX) {
          id.ms++;
          id.seq = 0;
        } else {
          id.seq++;
        }
        return Status::OK();
      }
    }

    // generate seq
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms) {
      LOG(ERROR) << "Time backwards detected !";
      return Status::InvalidArgument("The ID specified in XADD is equal or smaller");
    } else if (id.ms == last_id.ms) {
      if (last_id.seq == UINT64_MAX) {
        return Status::InvalidArgument("The ID specified in XADD is equal or smaller");
      }
      id.seq = last_id.seq + 1;
    } else {
      id.seq = 0;
    }

  } else {
    //  Full ID given, check id
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms || (id.ms == last_id.ms && id.seq <= last_id.seq)) {
      return Status::InvalidArgument("INVALID ID given");
    }
  }
}

Status RedisStreams::TrimByMaxlen(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                  const StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options) {
  Status s;
  // we delete the message in batchs, prevent from using too much memory
  while (stream_meta.length() - trim_ret.count > args.maxlen) {
    auto cur_batch =
        (std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count - args.maxlen), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<FieldValue> filed_values;

    RedisStreams::ScanStreamOptions options(key, stream_meta.first_id(), kSTREAMID_MAX, cur_batch, false, false, false);
    s = RedisStreams::ScanStream(options, filed_values, trim_ret.next_field, read_options);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }

    assert(filed_values.size() == cur_batch);
    trim_ret.count += cur_batch;
    trim_ret.max_deleted_field = filed_values.back().field;

    // delete the message in batchs
    std::vector<std::string> fields_to_del;
    fields_to_del.reserve(filed_values.size());
    for (auto& fv : filed_values) {
      fields_to_del.emplace_back(std::move(fv.field));
    }
    int32_t ret;
    s = DeleteStreamMessage(key, fields_to_del);
    if (!s.ok()) {
      return s;
    }
    assert(ret == fields_to_del.size());
  }

  s = Status::OK();
  return s;
}

Status RedisStreams::TrimByMinid(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                 const StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options) {
  Status s;
  std::string serialized_min_id;
  stream_meta.first_id().SerializeTo(trim_ret.next_field);
  args.minid.SerializeTo(serialized_min_id);

  // we delete the message in batchs, prevent from using too much memory
  while (trim_ret.next_field < serialized_min_id && stream_meta.length() - trim_ret.count > 0) {
    auto cur_batch = static_cast<int32_t>(
        std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<FieldValue> filed_values;

    RedisStreams::ScanStreamOptions options(key, stream_meta.first_id(), args.minid, cur_batch, false, false, false);
    s = RedisStreams::ScanStream(options, filed_values, trim_ret.next_field, read_options);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }

    if (!filed_values.empty()) {
      if (filed_values.back().field == serialized_min_id) {
        // we do not need to delete the message that it's id matches the minid
        filed_values.pop_back();
        trim_ret.next_field = serialized_min_id;
      }
      // duble check
      if (!filed_values.empty()) {
        trim_ret.max_deleted_field = filed_values.back().field;
      }
    }

    assert(filed_values.size() <= cur_batch);
    trim_ret.count += static_cast<int32_t>(filed_values.size());

    // do the delete in batch
    std::vector<std::string> fields_to_del;
    fields_to_del.reserve(filed_values.size());
    for (auto& fv : filed_values) {
      fields_to_del.emplace_back(std::move(fv.field));
    }
    int32_t ret;
    s = DeleteStreamMessage(key, fields_to_del);
    if (!s.ok()) {
      return s;
    }
    assert(ret == fields_to_del.size());
  }

  s = Status::OK();
  return s;
}

Status RedisStreams::ScanRange(const Slice& key, const Slice& id_start, const std::string& id_end, const Slice& pattern,
                               int32_t limit, std::vector<FieldValue>* id_messages, std::string* next_id,
                               rocksdb::ReadOptions& read_options) {
  assert(next_field);
  assert(field_values);
  next_id->clear();
  id_messages->clear();

  int64_t remain = limit;
  std::string meta_value;

  bool start_no_limit = id_start.compare("") == 0;
  bool end_no_limit = id_end.empty();

  if (!start_no_limit && !end_no_limit && (id_start.compare(id_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  StreamDataKey streams_data_prefix(key, Slice());
  StreamDataKey streams_start_data_key(key, id_start);
  std::string prefix = streams_data_prefix.Encode().ToString();
  rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
  for (iter->Seek(start_no_limit ? prefix : streams_start_data_key.Encode());
       iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Next()) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    std::string id = parsed_streams_data_key.id().ToString();
    if (!end_no_limit && id.compare(id_end) > 0) {
      break;
    }
    if (StringMatch(pattern.data(), pattern.size(), id.data(), id.size(), 0) != 0) {
      id_messages->push_back({id, iter->value().ToString()});
    }
    remain--;
  }

  if (iter->Valid() && iter->key().starts_with(prefix)) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    if (end_no_limit || parsed_streams_data_key.id().compare(id_end) <= 0) {
      *next_id = parsed_streams_data_key.id().ToString();
    }
  }
  delete iter;

  return Status::OK();
}

Status RedisStreams::ReScanRange(const Slice& key, const Slice& id_start, const std::string& id_end,
                                 const Slice& pattern, int32_t limit, std::vector<FieldValue>* id_messages,
                                 std::string* next_id, rocksdb::ReadOptions& read_options) {
  next_id->clear();
  id_messages->clear();

  int64_t remain = limit;
  std::string meta_value;

  bool start_no_limit = id_start.compare("") == 0;
  bool end_no_limit = id_end.empty();

  if (!start_no_limit && !end_no_limit && (id_start.compare(id_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  std::string start_key_id = start_no_limit ? "" : id_start.ToString();
  StreamDataKey streams_data_prefix(key, Slice());
  StreamDataKey streams_start_data_key(key, start_key_id);
  std::string prefix = streams_data_prefix.Encode().ToString();
  rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
  for (iter->SeekForPrev(streams_start_data_key.Encode().ToString());
       iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Prev()) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    std::string id = parsed_streams_data_key.id().ToString();
    if (!end_no_limit && id.compare(id_end) < 0) {
      break;
    }
    if (StringMatch(pattern.data(), pattern.size(), id.data(), id.size(), 0) != 0) {
      id_messages->push_back({id, iter->value().ToString()});
    }
    remain--;
  }

  if (iter->Valid() && iter->key().starts_with(prefix)) {
    ParsedStreamDataKey parsed_streams_data_key(iter->key());
    if (end_no_limit || parsed_streams_data_key.id().compare(id_end) >= 0) {
      *next_id = parsed_streams_data_key.id().ToString();
    }
  }
  delete iter;

  return Status::OK();
}
};  // namespace storage