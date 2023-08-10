
#include "include/pika_stream_util.h"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "include/pika_command.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "rocksdb/status.h"

bool StreamUtil::is_stream_meta_hash_created_ = false;

// Korpse TODO: test
CmdRes StreamUtil::ParseAddOrTrimArgs(const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int &idpos, bool is_xadd) {
  CmdRes res;
  int i = 2;
  bool limit_given = false;
  for (; i < argv.size(); ++i) {
    size_t moreargs = argv.size() - 1 - i;
    const std::string &opt = argv[i];
    if (is_xadd && strcasecmp(opt.c_str(), "*") == 0 && opt.size() == 1) {
      // case: XADD mystream * field value [field value ...]
      break;
    } else if (strcasecmp(opt.c_str(), "maxlen") == 0 && moreargs) {
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return res;
      }
      args.approx_trim = false;
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && next == "~") {
        // case: XADD mystream MAXLEN ~ <count> * field value [field value ...]
        args.approx_trim = true;
        i++;
      } else if (moreargs >= 2 && strcasecmp(next.c_str(), "=") == 0) {
        // case: XADD mystream MAXLEN = <count> * field value [field value ...]
        i++;
      }
      if (!StreamUtil::string2uint64(argv[i + 1].c_str(), args.maxlen)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid MAXLEN argument");
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MAXLEN;
      args.trim_strategy_arg_idx = i;
    } else if (strcasecmp(opt.c_str(), "minid") == 0 && moreargs) {
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return res;
      }
      args.approx_trim = false;
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && strcasecmp(next.c_str(), "~") == 0 && next.size() == 1) {
        // case: XADD mystream MINID ~ <id> * field value [field value ...]
        args.approx_trim = true;
        i++;
      } else if (moreargs >= 2 && strcasecmp(next.c_str(), "=") == 0 && next.size() == 1) {
        // case: XADD mystream MINID ~ <id> = field value [field value ...]
        i++;
      }
      auto ret = StreamUtil::StreamParseID(argv[i + 1], args.minid, 0);
      if (!ret.ok()) {
        res = ret;
        return res;
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MINID;
      args.trim_strategy_arg_idx = i;
    } else if (strcasecmp(opt.c_str(), "limit") == 0 && moreargs) {
      // case: XADD mystream ... LIMIT ...
      if (!StreamUtil::string2uint64(argv[i + 1].c_str(), args.limit)) {
        res.SetRes(CmdRes::kInvalidParameter);
        return res;
      }
      limit_given = true;
      i++;
    } else if (is_xadd && strcasecmp(opt.c_str(), "nomkstream") == 0) {
      // case: XADD mystream ... NOMKSTREAM ...
      args.no_mkstream = true;
    } else if (is_xadd) {
      // case: XADD mystream ... ID ...
      // FIXME: deal with seq_given
      auto ret = StreamUtil::StreamParseStrictID(argv[i], args.id, 0, &args.id_given);
      if (!ret.ok()) {
        res = ret;
        return res;
      }
      args.id_given = true;
    } else {
      res.SetRes(CmdRes::kSyntaxErr);
      return res;
    }
  }  // end for

  if (args.limit && args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_NONE) {
    res.SetRes(CmdRes::kSyntaxErr, "syntax error, LIMIT cannot be used without specifying a trimming strategy");
    return res;
  }

  if (!is_xadd && args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_NONE) {
    res.SetRes(CmdRes::kSyntaxErr, "syntax error, XTRIM must be called with a trimming strategy");
    return res;
  }

  // FIXME: figure out what is mustObeyClient() means in redis
  // if (mustObeyClient(c)) {
  //   args->limit = 0;
  // } else {

  if (limit_given) {
    if (!args.approx_trim) {
      res.SetRes(CmdRes::kSyntaxErr, "syntax error, LIMIT cannot be used without the special ~ option");
      return res;
    }
  } else {
    // if limit given but not give
    if (args.approx_trim) {
      // FIXME: let limit can be defined in config
      args.limit = 100 * 10000;
      if (args.limit <= 0) {
        args.limit = KSTREAM_MIN_LIMIT;
      }
      if (args.limit > kSTREAM_MAX_LIMIT) {
        args.limit = kSTREAM_MAX_LIMIT;
      }
    } else {
      args.limit = 0;
    }
  }

  idpos = i;
  return res;
}

rocksdb::Status StreamUtil::GetStreamMeta(const std::string &key, std::string &value,
                                          const std::shared_ptr<Slot> &slot) {
  return slot->db()->HGet(STREAM_META_HASH_KEY, key, &value);
}

// Korpse TODO: test
CmdRes StreamUtil::StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                        bool *seq_given) {
  CmdRes res;
  char buf[128];
  if (var.size() > sizeof(buf) - 1) {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return res;
  }

  memcpy(buf, var.data(), var.size());
  buf[var.size()] = '\0';

  if (strict && (buf[0] == '-' || buf[0] == '+') && buf[1] == '\0') {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return res;
  }

  if (seq_given != nullptr) {
    *seq_given = true;
  }

  if (buf[0] == '-' && buf[1] == '\0') {
    id.ms = 0;
    id.seq = 0;
    res.SetRes(CmdRes::kOk);
    return res;
  } else if (buf[0] == '+' && buf[1] == '\0') {
    id.ms = UINT64_MAX;
    id.seq = UINT64_MAX;
    res.SetRes(CmdRes::kOk);
    return res;
  }

  uint64_t ms;
  uint64_t seq;
  char *dot = strchr(buf, '-');
  if (dot) {
    *dot = '\0';
  }
  if (!string2uint64(buf, ms)) {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return res;
  };
  if (dot) {
    size_t seqlen = strlen(dot + 1);
    if (seq_given != nullptr && seqlen == 1 && *(dot + 1) == '*') {
      seq = 0;
      *seq_given = false;
    } else if (!string2uint64(dot + 1, seq)) {
      res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return res;
    }
  } else {
    seq = missing_seq;
  }
  id.ms = ms;
  id.seq = seq;
  res.SetRes(CmdRes::kOk);
  return res;
}

// Korpse TODO: test
CmdRes StreamUtil::StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq) {
  return StreamGenericParseID(var, id, missing_seq, false, nullptr);
}

// Korpse TODO: test
CmdRes StreamUtil::StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, bool *seq_given) {
  return StreamGenericParseID(var, id, missing_seq, true, seq_given);
}

// Korpse TODO: test
CmdRes StreamUtil::StreamParseIntervalId(const std::string &var, streamID &id, bool *exclude, uint64_t missing_seq) {
  if (exclude != nullptr) {
    *exclude = (var.size() > 1 && var[0] == '(');
  }
  if (exclude != nullptr && *exclude) {
    streamID tid;
    return StreamParseStrictID(var.substr(1), tid, missing_seq, nullptr);
  } else {
    return StreamParseID(var, id, missing_seq);
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

// no need to be thread safe, only xadd will call this function
// and xadd can be locked by the same key using current_key()
rocksdb::Status StreamUtil::InsertStreamMeta(const std::string &key, std::string &meta_value,
                                             const std::shared_ptr<Slot> &slot) {
  rocksdb::Status s;
  int32_t temp{0};
  s = slot->db()->HSet(STREAM_META_HASH_KEY, key, meta_value, &temp);
  (void)temp;
  if (!s.ok()) {
    LOG(FATAL) << "HSet failed, key: " << key << ", value: " << meta_value;
  }
  return s;
}

rocksdb::Status StreamUtil::InsertStreamMessage(const std::string &key, const std::string &sid,
                                                const std::string &message, const std::shared_ptr<Slot> &slot) {
  int32_t temp{0};
  rocksdb::Status s = slot->db()->HSet(key, sid, message, &temp);
  (void)temp;
  return s;
}

bool StreamUtil::SerializeMessage(const std::vector<std::string> &field_values, std::string &message, int field_pos) {
  assert(argv.size() - filed_pos >= 2 && (argv.size() - filed_pos) % 2 == 0);
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

bool StreamUtil::StreamID2String(const streamID &id, std::string &serialized_id) {
  assert(serialized_id.empty());
  serialized_id.reserve(sizeof(id));
  serialized_id.append(reinterpret_cast<const char *>(&id), sizeof(id));
  return true;
}

uint64_t StreamUtil::GetCurrentTimeMs() {
  uint64_t now =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  return now;
}

// Korpse TODO: test
/* XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds]
 * [NOACK] STREAMS key [key ...] id [id ...]
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id
 * [id ...] */
CmdRes StreamUtil::ParseReadOrReadGroupArgs(const PikaCmdArgsType &argv, StreamReadGroupReadArgs &args,
                                            bool is_xreadgroup) {
  CmdRes res;
  int streams_arg_idx{0};  // the index of stream keys arg
  size_t streams_cnt{0};   // the count of stream keys

  for (int i = 1; i < argv.size(); ++i) {
    size_t moreargs = argv.size() - i - 1;
    const std::string &o = argv[i];
    if (strcasecmp(o.c_str(), "BLOCK") == 0 && moreargs) {
      i++;
      if (!StreamUtil::string2uint64(argv[i].c_str(), args.block)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid BLOCK argument");
        return res;
      }
    } else if (strcasecmp(o.c_str(), "COUNT") == 0 && moreargs) {
      i++;
      if (!StreamUtil::string2int32(argv[i].c_str(), args.count)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid COUNT argument");
        return res;
      }
      if (args.count < 0) args.count = 0;
    } else if (strcasecmp(o.c_str(), "STREAMS") == 0 && moreargs) {
      streams_arg_idx = i + 1;
      streams_cnt = argv.size() - streams_arg_idx;
      if (streams_cnt % 2 != 0) {
        res.SetRes(CmdRes::kSyntaxErr, "Unbalanced list of streams: for each stream key an ID must be specified");
        return res;
      }
      streams_cnt /= 2;
    } else if (strcasecmp(o.c_str(), "GROUP") == 0 && moreargs >= 2) {
      if (!is_xreadgroup) {
        res.SetRes(CmdRes::kSyntaxErr, "The GROUP option is only supported by XREADGROUP. You called XREAD instead.");
        return res;
      }
      args.group_name = argv[i + 1];
      args.consumer_name = argv[i + 2];
      i += 2;
    } else if (strcasecmp(o.c_str(), "NOACK") == 0) {
      if (!is_xreadgroup) {
        res.SetRes(CmdRes::kSyntaxErr, "The NOACK option is only supported by XREADGROUP. You called XREAD instead.");
        return res;
      }
      args.noack_ = true;
    } else {
      res.SetRes(CmdRes::kSyntaxErr);
      return res;
    }
  }

  if (streams_arg_idx == 0) {
    res.SetRes(CmdRes::kSyntaxErr);
    return res;
  }

  if (is_xreadgroup && args.group_name.empty()) {
    res.SetRes(CmdRes::kSyntaxErr, "Missing GROUP option for XREADGROUP");
    return res;
  }

  // collect keys and ids
  for (auto i = streams_arg_idx + streams_cnt; i < argv.size(); ++i) {
    auto id_idx = i - streams_arg_idx - streams_cnt;
    auto key_idx = i - streams_cnt;
    args.keys.push_back(argv[key_idx]);
    args.unparsed_ids.push_back(argv[id_idx]);
    const std::string &key = argv[i - streams_cnt];
  }

  res.SetRes(CmdRes::kOk);
  return res;
}

rocksdb::Status StreamUtil::GetTreeNodeValue(const treeID tid, std::string &field, std::string &value,
                                             const std::shared_ptr<Slot> &slot) {
  std::string key;
  key.reserve(strlen(STERAM_TREE_PREFIX) + sizeof(tid));
  key.append(STERAM_TREE_PREFIX);
  key.append(reinterpret_cast<const char *>(&tid), sizeof(tid));

  rocksdb::Status s;
  s = slot->db()->HGet(key, field, &value);
  return s;
}

rocksdb::Status StreamUtil::InsertTreeNodeValue(const treeID tid, const std::string &filed,
                                                const std::string &value, const std::shared_ptr<Slot> &slot) {
  std::string key;
  key.reserve(strlen(STERAM_TREE_PREFIX) + sizeof(tid));
  key.append(STERAM_TREE_PREFIX);
  key.append(reinterpret_cast<const char *>(&tid), sizeof(tid));

  rocksdb::Status s;
  int res;
  s = slot->db()->HSet(key, filed, value, &res);
  (void)res;
  return s;
}