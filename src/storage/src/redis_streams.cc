#include "src/redis_streams.h"
#include <cstddef>
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

namespace storage {

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
  TRACE(FATAL, "RedisStreams::Expire not supported by stream");
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Expire not supported by stream"));
  return Status::Corruption(s.ToString());
}

bool RedisStreams::PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                                std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) {
  TRACE(FATAL, "RedisStreams::PKExpireScan not supported by stream");
  return false;
}

Status RedisStreams::Expireat(const Slice& key, int32_t timestamp) {
  TRACE(FATAL, "RedisStreams::Expireat not supported by stream");
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Expireat not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::Persist(const Slice& key) {
  TRACE(FATAL, "RedisStreams::Persist not supported by stream");
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::Persist not supported by stream"));
  return Status::Corruption(s.ToString());
}

Status RedisStreams::TTL(const Slice& key, int64_t* timestamp) {
  TRACE(FATAL, "RedisStreams::TTL not supported by stream");
  rocksdb::Status s(rocksdb::Status::NotSupported("RedisStreams::TTL not supported by stream"));
  return Status::Corruption(s.ToString());
}

storage::Status RedisStreams::GetStreamMeta(StreamMetaValue& stream_meta, const rocksdb::Slice& key) {
  std::string value;
  auto s = db_->Get(default_read_options_, handles_[0], key, &value);
  if (s.ok()) {
    stream_meta.ParseFrom(value);
    return storage::Status::OK();
  }
  return s;
}

storage::Status RedisStreams::SetStreamMeta(const rocksdb::Slice& key, std::string& meta_value) {
  return db_->Put(default_write_options_, handles_[0], key, meta_value);
}

storage::Status RedisStreams::InsertStreamMessage(const rocksdb::Slice& key, const streamID& id,
                                                  const std::string& message) {
  return db_->Put(default_write_options_, handles_[1], key, message);
}

storage::Status RedisStreams::GetStreamMessage(const rocksdb::Slice& key, const std::string& sid,
                                               std::string& message) {
  return db_->Get(default_read_options_, handles_[1], key, &message);
}

storage::Status RedisStreams::DeleteStreamMessage(const rocksdb::Slice& key, const std::vector<streamID>& id) {
  rocksdb::WriteBatch batch;
  for (auto& sid : id) {
    std::string serialized_id;
    sid.SerializeTo(serialized_id);
    batch.Delete(handles_[1], serialized_id);
  }
  return db_->Write(default_write_options_, &batch);
}

storage::Status RedisStreams::DeleteStreamMessage(const rocksdb::Slice& key,
                                                  const std::vector<std::string>& serialized_ids) {
  rocksdb::WriteBatch batch;
  for (auto& sid : serialized_ids) {
    batch.Delete(handles_[1], sid);
  }
  return db_->Write(default_write_options_, &batch);
}

storage::Status RedisStreams::TrimStream(int32_t& count, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                         Storage::StreamAddTrimArgs& args) {
  count = 0;
  // 1 do the trim
  TrimRet trim_ret;
  storage::Status s;
  if (args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MAXLEN) {
    s = TrimByMaxlen(trim_ret, stream_meta, key, args);
  } else {
    assert(args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MINID);
    s = TrimByMinid(trim_ret, stream_meta, key, args);
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

  s = storage::Status::OK();
  return s;
}

storage::Status RedisStreams::ScanStream(const ScanStreamOptions& op, std::vector<storage::FieldValue>& field_values,
                                         std::string& next_field) {
  std::string start_field;
  std::string end_field;
  storage::Slice pattern = "*";  // match all the fields from start_field to end_field
  storage::Status s;

  // 1 do the scan
  if (op.is_reverse) {
    op.end_sid.SerializeTo(start_field);
    if (op.start_sid == kSTREAMID_MAX) {
      start_field = "";
    } else {
      op.start_sid.SerializeTo(start_field);
    }
    s = db->PKHRScanRange(true_key, start_field, end_field, pattern, op.count, &field_values, &next_field);
  } else {
    op.start_sid.SerializeTo(start_field);
    if (op.end_sid == kSTREAMID_MAX) {
      end_field = "";
    } else {
      op.end_sid.SerializeTo(end_field);
    }
    s = db->PKHScanRange(true_key, start_field, end_field, pattern, op.count, &field_values, &next_field);
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
storage::Status RedisStreams::DeleteStreamData(const rocksdb::Slice& key) {
  return db_->Delete(default_write_options_, handles_[1], key);
}

Status RedisStreams::GenerateStreamID(const StreamMetaValue& stream_meta, Storage::StreamAddTrimArgs& args) {
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

storage::Status RedisStreams::TrimByMaxlen(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                           const Storage::StreamAddTrimArgs& args) {
  storage::Status s;
  // we delete the message in batchs, prevent from using too much memory
  while (stream_meta.length() - trim_ret.count > args.maxlen) {
    auto cur_batch =
        (std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count - args.maxlen), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<storage::FieldValue> filed_values;

    RedisStreams::ScanStreamOptions options(key, stream_meta.first_id(), kSTREAMID_MAX, cur_batch, false, false, false);
    s = RedisStreams::ScanStream(options, filed_values, trim_ret.next_field);
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

  s = storage::Status::OK();
  return s;
}

storage::Status RedisStreams::TrimByMinid(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                                          const Storage::StreamAddTrimArgs& args) {
  storage::Status s;
  std::string serialized_min_id;
  stream_meta.first_id().SerializeTo(trim_ret.next_field);
  args.minid.SerializeTo(serialized_min_id);

  // we delete the message in batchs, prevent from using too much memory
  while (trim_ret.next_field < serialized_min_id && stream_meta.length() - trim_ret.count > 0) {
    auto cur_batch = static_cast<int32_t>(
        std::min(static_cast<int32_t>(stream_meta.length() - trim_ret.count), kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<storage::FieldValue> filed_values;

    RedisStreams::ScanStreamOptions options(key, stream_meta.first_id(), args.minid, cur_batch, false, false, false);
    s = RedisStreams::ScanStream(options, filed_values, trim_ret.next_field);
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

  s = storage::Status::OK();
  return s;
}

};  // namespace storage