
#include "include/pika_stream_util.h"
#include <cassert>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <vector>
#include "include/pika_command.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "rocksdb/status.h"
#include "src/coding.h"
#include "storage/storage.h"

bool StreamUtil::is_stream_meta_hash_created_ = false;

treeID TreeIDGenerator::GetNextTreeID(const std::shared_ptr<Slot> &slot) {
  treeID expected_id = kINVALID_TREE_ID;
  if (tree_id_.compare_exchange_strong(expected_id, START_TREE_ID)) {
    TryToFetchLastIdFromStorage(slot);
  }
  assert(tree_id_ != kINVALID_TREE_ID);
  ++tree_id_;
  std::string tree_id_str = std::to_string(tree_id_);
  rocksdb::Status s = slot->db()->Set(STREAM_TREE_STRING_KEY, tree_id_str);
  LOG(INFO) << "Set tree id to " << tree_id_str << " tree id: " << tree_id_;
  return tree_id_;
}

void TreeIDGenerator::TryToFetchLastIdFromStorage(const std::shared_ptr<Slot> &slot) {
  std::string value;
  rocksdb::Status s = slot->db()->Get(STREAM_TREE_STRING_KEY, &value);
  if (s.ok()) {
    // found, set tree id
    auto id = std::stoi(value);
    if (id < START_TREE_ID) {
      LOG(FATAL) << "Invalid tree id: " << id << ", set to start tree id: " << START_TREE_ID;
    }

  } else {
    // not found, set start tree id and insert to db
    tree_id_ = START_TREE_ID;
    LOG(INFO) << "Tree id not found, set to start tree id: " << START_TREE_ID;
  }
}

void StreamUtil::ParseAddOrTrimArgsOrRep(CmdRes &res, const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int *idpos,
                                         bool is_xadd) {
  int i = 2;
  bool limit_given = false;
  for (; i < argv.size(); ++i) {
    size_t moreargs = argv.size() - 1 - i;
    const std::string &opt = argv[i];

    if (is_xadd && strcasecmp(opt.c_str(), "*") == 0 && opt.size() == 1) {
      // case: XADD mystream * field value [field value ...]
      break;

    } else if (strcasecmp(opt.c_str(), "maxlen") == 0 && moreargs) {
      // case: XADD mystream ... MAXLEN [= | ~] threshold ...
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && (next == "~" || next == "=")) {
        // we allways not do approx trim, so we ignore the ~ and =
        i++;
      }
      // parse threshold as uint64
      if (!StreamUtil::string2uint64(argv[i + 1].c_str(), args.maxlen)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid MAXLEN argument");
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MAXLEN;
      args.trim_strategy_arg_idx = i;

    } else if (strcasecmp(opt.c_str(), "minid") == 0 && moreargs) {
      // case: XADD mystream ... MINID [= | ~] threshold ...
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && (next == "~" || next == "=") && next.size() == 1) {
        // we allways not do approx trim, so we ignore the ~ and =
        i++;
      }
      // parse threshold as stremID
      StreamUtil::StreamParseIDOrRep(res, argv[i + 1], args.minid, 0);
      if (res.ret() != CmdRes::kNone) {
        return;
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MINID;
      args.trim_strategy_arg_idx = i;

    } else if (strcasecmp(opt.c_str(), "limit") == 0 && moreargs) {
      // case: XADD mystream ... ~ threshold LIMIT count ...
      // we do not need approx trim, so we do not support LIMIT option
      res.SetRes(CmdRes::kSyntaxErr, "syntax error, Pika do not support LIMIT option");
      return;

    } else if (is_xadd && strcasecmp(opt.c_str(), "nomkstream") == 0) {
      // case: XADD mystream ... NOMKSTREAM ...
      args.no_mkstream = true;

    } else if (is_xadd) {
      // case: XADD mystream ... ID ...
      StreamUtil::StreamParseStrictIDOrRep(res, argv[i], args.id, 0, &args.seq_given);
      if (res.ret() != CmdRes::kNone) {
        return;
      }
      args.id_given = true;
      break;
    } else {
      res.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }  // end for

  if (idpos) {
    *idpos = i;
  } else if (is_xadd) {
    LOG(ERROR) << "idpos is null, xadd comand must parse idpos";
  }
}

rocksdb::Status StreamUtil::GetStreamMeta(const std::string &key, std::string &value,
                                          const std::shared_ptr<Slot> &slot) {
  return slot->db()->HGet(STREAM_META_HASH_KEY, key, &value);
}

rocksdb::Status StreamUtil::GetStreamMeta(StreamMetaValue *stream_meta, const std::string &key,
                                          const std::shared_ptr<Slot> &slot) {
  assert(stream_meta);
  std::string value;
  auto s = slot->db()->HGet(STREAM_META_HASH_KEY, key, &value);
  if (s.ok()) {
    stream_meta->ParseFrom(value);
    return rocksdb::Status::OK();
  }
  return s;
}

// no need to be thread safe, only xadd will call this function
// and xadd can be locked by the same key using current_key()
rocksdb::Status StreamUtil::UpdateStreamMeta(const std::string &key, std::string &meta_value,
                                             const std::shared_ptr<Slot> &slot) {
  rocksdb::Status s;
  int32_t temp{0};
  s = slot->db()->HSet(STREAM_META_HASH_KEY, key, meta_value, &temp);
  (void)temp;
  return s;
}

// Korpse TODO: test
void StreamUtil::StreamGenericParseIDOrRep(CmdRes &res, const std::string &var, streamID &id, uint64_t missing_seq,
                                           bool strict, bool *seq_given) {
  char buf[128];
  if (var.size() > sizeof(buf) - 1) {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return;
  }

  memcpy(buf, var.data(), var.size());
  buf[var.size()] = '\0';

  if (strict && (buf[0] == '-' || buf[0] == '+') && buf[1] == '\0') {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return;
  }

  if (seq_given != nullptr) {
    *seq_given = true;
  }

  if (buf[0] == '-' && buf[1] == '\0') {
    id.ms = 0;
    id.seq = 0;
    return;
  } else if (buf[0] == '+' && buf[1] == '\0') {
    id.ms = UINT64_MAX;
    id.seq = UINT64_MAX;
    return;
  }

  uint64_t ms;
  uint64_t seq;
  char *dot = strchr(buf, '-');
  if (dot) {
    *dot = '\0';
  }
  if (!string2uint64(buf, ms)) {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return;
  };
  if (dot) {
    size_t seqlen = strlen(dot + 1);
    if (seq_given != nullptr && seqlen == 1 && *(dot + 1) == '*') {
      seq = 0;
      *seq_given = false;
    } else if (!string2uint64(dot + 1, seq)) {
      res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return;
    }
  } else {
    seq = missing_seq;
  }
  id.ms = ms;
  id.seq = seq;
}

// Korpse TODO: test
void StreamUtil::StreamParseIDOrRep(CmdRes &res, const std::string &var, streamID &id, uint64_t missing_seq) {
  StreamGenericParseIDOrRep(res, var, id, missing_seq, false, nullptr);
}

// Korpse TODO: test
void StreamUtil::StreamParseStrictIDOrRep(CmdRes &res, const std::string &var, streamID &id, uint64_t missing_seq,
                                          bool *seq_given) {
  StreamGenericParseIDOrRep(res, var, id, missing_seq, true, seq_given);
}

// Korpse TODO: test
void StreamUtil::StreamParseIntervalIdOrRep(CmdRes &res, const std::string &var, streamID &id, bool *exclude,
                                            uint64_t missing_seq) {
  if (exclude != nullptr) {
    *exclude = (var.size() > 1 && var[0] == '(');
  }
  if (exclude != nullptr && *exclude) {
    StreamParseStrictIDOrRep(res, var.substr(1), id, missing_seq, nullptr);
  } else {
    StreamParseIDOrRep(res, var, id, missing_seq);
  }
}

bool StreamUtil::string2uint64(const char *s, uint64_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  uint64_t tmp = strtoull(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtil::string2int64(const char *s, int64_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  int64_t tmp = std::strtoll(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtil::string2int32(const char *s, int32_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  long tmp = strtol(s, &end, 10);
  if (*end || errno == ERANGE || tmp < INT_MIN || tmp > INT_MAX) {
    // Conversion either didn't consume the entire string,
    // or overflow or underflow occurred
    return false;
  }

  value = static_cast<int32_t>(tmp);
  return true;
}

void StreamUtil::DeleteStreamMeta(const std::string &key, const std::shared_ptr<Slot> &slot) {
  int32_t ret;
  auto s = slot->db()->HDel({STREAM_META_HASH_KEY}, {key}, &ret);
  (void)ret;
  if (!s.ok() && !s.IsNotFound()) {
    LOG(WARNING) << "Delete stream meta failed, key: " << key << ", error: " << s.ToString();
  }
}

rocksdb::Status StreamUtil::InsertStreamMessage(const std::string &key, const streamID &id, const std::string &message,
                                                const std::shared_ptr<Slot> &slot) {
  int32_t temp{0};
  std::string serialized_id;
  id.SerializeTo(serialized_id);
  rocksdb::Status s = slot->db()->HSet(key, serialized_id, message, &temp);
  (void)temp;
  return s;
}

rocksdb::Status StreamUtil::DeleteStreamMessage(const std::string &key, const std::vector<streamID> &id, int32_t &ret,
                                                const std::shared_ptr<Slot> &slot) {
  std::vector<std::string> serialized_ids;
  for (auto &sid : id) {
    std::string serialized_id;
    sid.SerializeTo(serialized_id);
    serialized_ids.emplace_back(std::move(serialized_id));
  }
  return slot->db()->HDel(key, {serialized_ids}, &ret);
}

bool StreamUtil::SerializeMessage(const std::vector<std::string> &field_values, std::string &message, int field_pos) {
  assert(field_values.size() - field_pos >= 2 && (field_values.size() - field_pos) % 2 == 0);
  assert(message.empty());
  // count the size of serizlized message
  size_t size = 0;
  for (int i = field_pos; i < field_values.size(); i++) {
    size += field_values[i].size() + sizeof(size_t);
  }
  message.reserve(size);

  // serialize message
  for (int i = field_pos; i < field_values.size(); i++) {
    size_t len = field_values[i].size();
    message.append(reinterpret_cast<const char *>(&len), sizeof(len));
    message.append(field_values[i]);
  }

  return true;
}

bool StreamUtil::DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message) {
  size_t pos = 0;
  while (pos < message.size()) {
    // Read the length of the next field value from the message
    size_t len = *reinterpret_cast<const size_t *>(&message[pos]);
    pos += sizeof(size_t);

    // Check if the calculated end of the string is still within the message bounds
    if (pos + len > message.size()) {
      LOG(ERROR) << "Invalid message format, failed to parse message";
      return false;  // Error: not enough data in the message string
    }

    // Extract the field value and add it to the vector
    parsed_message.push_back(message.substr(pos, len));
    pos += len;
  }

  return true;
}

inline void StreamUtil::ScanStreamOrRep(CmdRes &res, const ScanStreamOptions &option,
                                        std::vector<storage::FieldValue> &field_values, std::string &next_field,
                                        const std::shared_ptr<Slot> &slot) {
  auto &skey = option.skey;
  auto &start_sid = option.start_sid;
  auto &end_sid = option.end_sid;
  auto count = option.count;

  std::string start_field;
  std::string end_field;
  rocksdb::Slice pattern = "*";  // match all the fields from start_field to end_field
  start_sid.SerializeTo(start_field);
  if (end_sid == kSTREAMID_MAX) {
    end_field = "";  // empty for no end_sid
  } else {
    end_sid.SerializeTo(end_field);
  }

  rocksdb::Status s =
      slot->db()->PKHScanRange(skey, start_field, end_field, pattern, count, &field_values, &next_field);
  if (s.IsNotFound()) {
    LOG(INFO) << "no message found in XRange";
    res.AppendArrayLen(0);
    return;
  } else if (!s.ok()) {
    LOG(ERROR) << "PKHScanRange failed";
    res.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
}

uint64_t StreamUtil::GetCurrentTimeMs() {
  uint64_t now =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  return now;
}

void StreamUtil::ScanAndAppendMessageToResOrRep(CmdRes &res, const ScanStreamOptions &op,
                                                const std::shared_ptr<Slot> &slot, std::vector<std::string> *row_ids,
                                                bool start_ex, bool end_ex) {
  std::vector<storage::FieldValue> field_values;
  std::string next_field;
  auto &start_sid = op.start_sid;
  auto &end_sid = op.end_sid;

  StreamUtil::ScanStreamOrRep(res, op, field_values, next_field, slot);
  (void)next_field;
  if (res.ret() != CmdRes::kNone) {
    return;
  }

  if (start_ex && !field_values.empty()) {
    streamID sid;
    sid.DeserializeFrom(field_values.front().field);
    if (sid == start_sid) {
      field_values.erase(field_values.begin());
    }
  }

  if (end_ex && !field_values.empty()) {
    streamID sid;
    sid.DeserializeFrom(field_values.back().field);
    if (sid == end_sid) {
      field_values.pop_back();
    }
  }

  // append the result to res_
  // the outer layer is an array, each element is a inner array witch has 2 elements
  // the inner array's first element is the field, the second element is an array of messages
  LOG(INFO) << "XRange Found " << field_values.size() << " messages";
  res.AppendArrayLenUint64(field_values.size());
  for (auto &fv : field_values) {
    // if ids is not null, we need to record the id of each message
    if (row_ids) {
      row_ids->push_back(fv.field);
    }

    std::vector<std::string> message;
    if (!DeserializeMessage(fv.value, message)) {
      LOG(ERROR) << "Deserialize message failed";
      res.SetRes(CmdRes::kErrOther, "Deserialize message failed");
      return;
    }

    assert(message.size() % 2 == 0);
    res.AppendArrayLen(2);
    // FIXME: make the stream id readable
    streamID sid;
    sid.DeserializeFrom(fv.field);
    res.AppendString(sid.ToString());  // field here is the stream id
    res.AppendArrayLenUint64(message.size());
    for (auto &m : message) {
      res.AppendString(m);
    }
  }
}

// Korpse TODO: test
/* XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds]
 * [NOACK] STREAMS key [key ...] id [id ...]
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id
 * [id ...] */
void StreamUtil::ParseReadOrReadGroupArgsOrRep(CmdRes &res, const PikaCmdArgsType &argv, StreamReadGroupReadArgs &args,
                                               bool is_xreadgroup) {
  int streams_arg_idx{0};  // the index of stream keys arg
  size_t streams_cnt{0};   // the count of stream keys

  for (int i = 1; i < argv.size(); ++i) {
    size_t moreargs = argv.size() - i - 1;
    const std::string &o = argv[i];
    if (strcasecmp(o.c_str(), "BLOCK") == 0 && moreargs) {
      i++;
      if (!StreamUtil::string2uint64(argv[i].c_str(), args.block)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid BLOCK argument");
        return;
      }
    } else if (strcasecmp(o.c_str(), "COUNT") == 0 && moreargs) {
      i++;
      if (!StreamUtil::string2int32(argv[i].c_str(), args.count)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid COUNT argument");
        return;
      }
      if (args.count < 0) args.count = 0;
    } else if (strcasecmp(o.c_str(), "STREAMS") == 0 && moreargs) {
      streams_arg_idx = i + 1;
      streams_cnt = argv.size() - streams_arg_idx;
      if (streams_cnt % 2 != 0) {
        res.SetRes(CmdRes::kSyntaxErr, "Unbalanced list of streams: for each stream key an ID must be specified");
        return;
      }
      streams_cnt /= 2;
      break;
    } else if (strcasecmp(o.c_str(), "GROUP") == 0 && moreargs >= 2) {
      if (!is_xreadgroup) {
        res.SetRes(CmdRes::kSyntaxErr, "The GROUP option is only supported by XREADGROUP. You called XREAD instead.");
        return;
      }
      args.group_name = argv[i + 1];
      args.consumer_name = argv[i + 2];
      i += 2;
    } else if (strcasecmp(o.c_str(), "NOACK") == 0) {
      if (!is_xreadgroup) {
        res.SetRes(CmdRes::kSyntaxErr, "The NOACK option is only supported by XREADGROUP. You called XREAD instead.");
        return;
      }
      args.noack_ = true;
    } else {
      res.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }

  if (streams_arg_idx == 0) {
    res.SetRes(CmdRes::kSyntaxErr);
    return;
  }

  if (is_xreadgroup && args.group_name.empty()) {
    res.SetRes(CmdRes::kSyntaxErr, "Missing GROUP option for XREADGROUP");
    return;
  }

  // collect keys and ids
  for (auto i = streams_arg_idx + streams_cnt; i < argv.size(); ++i) {
    auto key_idx = i - streams_cnt;
    args.keys.push_back(argv[key_idx]);
    args.unparsed_ids.push_back(argv[i]);
    const std::string &key = argv[i - streams_cnt];
  }
}

inline StreamUtil::TrimRet StreamUtil::TrimByMaxlenOrRep(StreamMetaValue &stream_meta, const std::string &key,
                                                         const std::shared_ptr<Slot> &slot, CmdRes &res,
                                                         const StreamAddTrimArgs &args) {
  TrimRet trim_ret;
  // we delete the message in batchs, prevent from using too much memory
  while (stream_meta.length() - trim_ret.count > args.maxlen) {
    auto cur_batch =
        static_cast<int32_t>(std::min(stream_meta.length() - trim_ret.count - args.maxlen, kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<storage::FieldValue> filed_values;

    ScanStreamOptions options(key, stream_meta.first_id(), kSTREAMID_MAX, cur_batch);
    StreamUtil::ScanStreamOrRep(res, options, filed_values, trim_ret.next_field, slot);

    if (res.ret() != CmdRes::kNone) {
      return trim_ret;
    }
    assert(filed_values.size() == cur_batch);
    trim_ret.count += cur_batch;
    trim_ret.max_deleted_field = filed_values.back().field;

    // delete the message in batchs
    std::vector<std::string> fields_to_del;
    fields_to_del.reserve(filed_values.size());
    for (auto &fv : filed_values) {
      fields_to_del.emplace_back(std::move(fv.field));
    }
    int32_t ret;
    auto s = slot->db()->HDel(key, fields_to_del, &ret);
    if (!s.ok()) {
      res.SetRes(CmdRes::kErrOther, s.ToString());
      return trim_ret;
    }
    assert(ret == fields_to_del.size());
  }

  return trim_ret;
}

inline StreamUtil::TrimRet StreamUtil::TrimByMinidOrRep(StreamMetaValue &stream_meta, const std::string &key,
                                                        const std::shared_ptr<Slot> &slot, CmdRes &res,
                                                        const StreamAddTrimArgs &args) {
  TrimRet trim_ret;
  std::string serialized_min_id;
  stream_meta.first_id().SerializeTo(trim_ret.next_field);
  args.minid.SerializeTo(serialized_min_id);

  // we delete the message in batchs, prevent from using too much memory
  while (trim_ret.next_field < serialized_min_id && stream_meta.length() - trim_ret.count > 0) {
    auto cur_batch = static_cast<int32_t>(std::min(stream_meta.length() - trim_ret.count, kDEFAULT_TRIM_BATCH_SIZE));
    std::vector<storage::FieldValue> filed_values;
    ScanStreamOptions options(key, stream_meta.first_id(), args.minid, cur_batch);
    StreamUtil::ScanStreamOrRep(res, options, filed_values, trim_ret.next_field, slot);
    if (!res.ok()) {
      return trim_ret;
    }

    // FIXME: should I do some check here?
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

    trim_ret.count += static_cast<int32_t>(filed_values.size());

    // do the delete in batch
    std::vector<std::string> fields_to_del;
    fields_to_del.reserve(filed_values.size());
    for (auto &fv : filed_values) {
      fields_to_del.emplace_back(std::move(fv.field));
    }
    int32_t ret;
    auto s = slot->db()->HDel(key, fields_to_del, &ret);
    if (!s.ok()) {
      res.SetRes(CmdRes::kErrOther, s.ToString());
      return trim_ret;
    }
    assert(ret == fields_to_del.size());
  }

  return trim_ret;
}

int32_t StreamUtil::TrimStreamOrRep(CmdRes &res, StreamMetaValue &stream_meta, const std::string &key,
                                    StreamAddTrimArgs &args, const std::shared_ptr<Slot> &slot) {
  // 1 do the trim
  TrimRet trim_ret;
  if (args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MAXLEN) {
    trim_ret = TrimByMaxlenOrRep(stream_meta, key, slot, res, args);
  } else if (args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_MINID) {
    trim_ret = TrimByMinidOrRep(stream_meta, key, slot, res, args);
  } else {
    LOG(ERROR) << "Invalid trim strategy";
    res.SetRes(CmdRes::kErrOther, "Invalid trim strategy");
    return 0;
  }

  if (res.ret() != CmdRes::kNone) {
    LOG(ERROR) << "Trim stream failed";
    return 0;
  }

  if (trim_ret.count == 0) {
    return 0;
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

  return trim_ret.count;
}

std::string TreeID2Key(const treeID &tid) {
  std::string key;
  key.reserve(strlen(STERAM_TREE_PREFIX) + sizeof(tid));
  key.append(STERAM_TREE_PREFIX);
  key.append(reinterpret_cast<const char *>(&tid), sizeof(tid));
  return key;
}

rocksdb::Status StreamUtil::GetTreeNodeValue(const treeID tid, std::string &field, std::string &value,
                                             const std::shared_ptr<Slot> &slot) {
  auto key = std::move(TreeID2Key(tid));
  rocksdb::Status s;
  s = slot->db()->HGet(key, field, &value);
  return s;
}

rocksdb::Status StreamUtil::InsertTreeNodeValue(const treeID tid, const std::string &filed, const std::string &value,
                                                const std::shared_ptr<Slot> &slot) {
  auto key = std::move(TreeID2Key(tid));

  rocksdb::Status s;
  int res;
  s = slot->db()->HSet(key, filed, value, &res);
  (void)res;
  return s;
}

rocksdb::Status StreamUtil::DeleteTreeNode(const treeID tid, const std::string &field,
                                           const std::shared_ptr<Slot> &slot) {
  auto key = std::move(TreeID2Key(tid));

  rocksdb::Status s;
  int res;
  s = slot->db()->HDel(key, std::vector<std::string>{field}, &res);
  (void)res;
  return s;
}

// can be used when delete all the consumer of a cgroup
// FIXME: what if the memory is not enough to hold all the node?
rocksdb::Status StreamUtil::GetAllTreeNode(const treeID tid, std::vector<storage::FieldValue> &field_values,
                                           const std::shared_ptr<Slot> &slot) {
  auto key = std::move(TreeID2Key(tid));
  return slot->db()->PKHScanRange(key, "", "", "*", INT_MAX, &field_values, nullptr);
}

bool StreamUtil::DeleteTree(const treeID tid, const std::shared_ptr<Slot> &slot) {
  assert(tid != kINVALID_TREE_ID);
  auto key = std::move(TreeID2Key(tid));
  std::map<storage::DataType, storage::Status> type_status;
  int64_t count = slot->db()->Del({key}, &type_status);
  auto s = type_status[storage::DataType::kStrings];
  if (!s.ok() || count == 0) {
    LOG(ERROR) << "DeleteTree failed, key: " << key << ", count: " << count;
    return false;
  }
  return true;
}

void StreamUtil::DestoryStreams(std::vector<std::string> &keys, const std::shared_ptr<Slot> &slot) {
  for (const auto &key : keys) {
    // 1.try to get the stream meta
    std::string meta_value;
    auto s = StreamUtil::GetStreamMeta(key, meta_value, slot);
    if (s.IsNotFound()) {
      LOG(INFO) << "Stream meta not found, skip key: " << key;
      continue;
    } else if (!s.ok()) {
      LOG(ERROR) << "Get stream meta failed";
      continue;
    }
    StreamMetaValue stream_meta;
    stream_meta.ParseFrom(meta_value);

    LOG(INFO) << "Deleting stream: " << key << " meta: " << stream_meta.ToString();

    // 2 destroy all the cgroup
    // 2.1 find all the cgroups' meta
    auto cgroup_tid = stream_meta.groups_id();
    if (cgroup_tid == kINVALID_TREE_ID) {
      LOG(INFO) << "No cgroup found, skip";
    } else {
      std::vector<storage::FieldValue> field_values;
      s = GetAllTreeNode(cgroup_tid, field_values, slot);
      if (!s.ok() && !s.IsNotFound()) {
        LOG(ERROR) << "Get all cgroups failed";
        continue;
      }
      // 2.2 loop all the cgroups, and destroy them
      for (auto &fv : field_values) {
        StreamUtil::DestoryCGroup(cgroup_tid, fv.field, slot);
      }
      // 2.3 delete the cgroup tree
      StreamUtil::DeleteTree(cgroup_tid, slot);
    }

    // 3 delete stream meta
    StreamUtil::DeleteStreamMeta(key, slot);
  }
}

bool StreamUtil::CreateConsumer(treeID consumer_tid, std::string &consumername, const std::shared_ptr<Slot> &slot) {
  std::string consumer_meta_value;
  auto s = StreamUtil::GetTreeNodeValue(consumer_tid, consumername, consumer_meta_value, slot);
  if (s.IsNotFound()) {
    LOG(INFO) << "Consumer meta not found, create new one";
    auto &tid_gen = TreeIDGenerator::GetInstance();
    auto pel_tid = tid_gen.GetNextTreeID(slot);
    StreamConsumerMetaValue consumer_meta;
    consumer_meta.Init(pel_tid);
    s = StreamUtil::InsertTreeNodeValue(consumer_tid, consumername, consumer_meta.value(), slot);
    if (!s.ok()) {
      LOG(ERROR) << "Insert consumer meta failed";
      return false;
    }
    return true;
  }
  // consumer meta already exists or other error
  return false;
}

rocksdb::Status StreamUtil::DestoryCGroup(treeID cgroup_tid, std::string &cgroupname,
                                          const std::shared_ptr<Slot> &slot) {
  // 1.get the cgroup meta
  std::string cgroup_meta_value;
  auto s = StreamUtil::GetTreeNodeValue(cgroup_tid, cgroupname, cgroup_meta_value, slot);
  if (s.IsNotFound()) {
    return s;
  } else if (!s.ok()) {
    LOG(ERROR) << "Get consumer meta failed, cgroup: " << cgroupname << ", cgroup_tid: " << cgroup_tid;
    return s;
  }
  StreamConsumerMetaValue cgroup_meta;
  cgroup_meta.ParseFrom(cgroup_meta_value);

  // 2 delete all the consumers
  // 2.1 get all the consumer meta
  auto consumer_tid = cgroup_meta.pel_tid();
  std::vector<storage::FieldValue> field_values;
  s = StreamUtil::GetAllTreeNode(consumer_tid, field_values, slot);
  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "Get all consumer meta failed"
               << ", cgroup: " << cgroupname << ", cgroup_tid: " << cgroup_tid;
    return s;
  }
  // 2.2 for each consumer, delete the pel of the consumer
  for (auto &field_value : field_values) {
    StreamConsumerMetaValue consumer_meta;
    consumer_meta.ParseFrom(field_value.value);
    auto pel_tid = consumer_meta.pel_tid();
    StreamUtil::DeleteTree(pel_tid, slot);
  }
  // 2.3 delete the consumer tree
  if (!StreamUtil::DeleteTree(consumer_tid, slot)) {
    return storage::Status::Corruption();
  }

  // 3 delete the pel of the cgroup
  auto pel_tid = cgroup_meta.pel_tid();
  if (!StreamUtil::DeleteTree(consumer_tid, slot)) {
    return storage::Status::Corruption();
  }

  return storage::Status::OK();
}