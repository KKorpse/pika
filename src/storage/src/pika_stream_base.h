// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <cstddef>
#include "include/storage/storage.h"
#include "pika_stream_meta_value.h"
#include "pika_stream_types.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"

namespace storage {

// each abstracted tree has a prefix,
// prefix + treeID will be the key of the hash to store the tree,
// notice: we need to ban the use of this prefix when using "HSET".
static const std::string STERAM_TREE_PREFIX = "STR_TREE";

// I need to find a place to store the last generated tree id,
// so I stored it as a field-value pair in the hash storing stream meta.
// the field is a fixed string, and the value is the last generated tree id.
// notice: we need to ban the use of this key when using stream, to avoid conflict with stream's key.
static const std::string STREAM_LAST_GENERATED_TREE_ID_FIELD = "STREAM";

// the max number of each delete operation in XTRIM command，to avoid too much memory usage.
// eg. if a XTIRM command need to trim 10000 items, the implementation will use rocsDB's delete operation (10000 /
// kDEFAULT_TRIM_BATCH_SIZE) times
const static int32_t kDEFAULT_TRIM_BATCH_SIZE = 1000;
struct StreamAddTrimArgs {
  // XADD options
  streamID id;
  bool id_given{false};
  bool seq_given{false};
  bool no_mkstream{false};

  // XADD + XTRIM common options
  StreamTrimStrategy trim_strategy{TRIM_STRATEGY_NONE};
  int trim_strategy_arg_idx{0};

  // TRIM_STRATEGY_MAXLEN options
  uint64_t maxlen{0};
  streamID minid;
};

struct StreamReadGroupReadArgs {
  // XREAD + XREADGROUP common options
  std::vector<std::string> keys;
  std::vector<std::string> unparsed_ids;
  int32_t count{INT32_MAX};  // The limit of read, in redis this is uint64_t, but PKHScanRange only support int32_t
  uint64_t block{0};         // 0 means no block

  // XREADGROUP options
  std::string group_name;
  std::string consumer_name;
  bool noack_{false};
};

struct StreamScanArgs {
  streamID start_sid;
  streamID end_sid;
  size_t limit{INT32_MAX};
  bool start_ex{false};    // exclude first message
  bool end_ex{false};      // exclude last message
  bool is_reverse{false};  // scan in reverse order
};

// get next tree id thread safe
class TreeIDGenerator {
 private:
  TreeIDGenerator() = default;
  void operator=(const TreeIDGenerator &) = delete;

 public:
  ~TreeIDGenerator() = default;

  // work in singeletone mode
  static TreeIDGenerator &GetInstance() {
    static TreeIDGenerator instance;
    return instance;
  }

  storage::Status GetNextTreeID(const rocksdb::DB *db, treeID &tid);

 private:
  static const treeID START_TREE_ID = 0;
  std::atomic<treeID> tree_id_{kINVALID_TREE_ID};
};

// Helper function of stream command.
// Should be reconstructed when transfer to another command framework.
// any function that has Reply in its name will reply to the client if error occurs.
class StreamCmdBase {
 public:
 private:
  StreamCmdBase();
  ~StreamCmdBase();
};

class StreamUtils {
 public:
  StreamUtils() = default;
  ~StreamUtils() = default;

  static bool string2uint64(const char *s, uint64_t &value);
  static bool string2int64(const char *s, int64_t &value);
  static bool string2int32(const char *s, int32_t &value);
  static std::string TreeID2Key(const treeID &tid);

  static uint64_t GetCurrentTimeMs();

  // serialize the message to a string.
  // format: {field1.size, field1, value1.size, value1, field2.size, field2, ...}
  static bool SerializeMessage(const std::vector<std::string> &field_values, std::string &serialized_message,
                               int field_pos);

  // deserialize the message from a string with the format of SerializeMessage.
  static bool DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message);

  // Parse a stream ID in the format given by clients to Pika, that is
  // <ms>-<seq>, and converts it into a streamID structure. The ID may be in incomplete
  // form, just stating the milliseconds time part of the stream. In such a case
  // the missing part is set according to the value of 'missing_seq' parameter.
  //
  // The IDs "-" and "+" specify respectively the minimum and maximum IDs
  // that can be represented. If 'strict' is set to 1, "-" and "+" will be
  // treated as an invalid ID.
  //
  // The ID form <ms>-* specifies a millisconds-only ID, leaving the sequence part
  // to be autogenerated. When a non-NULL 'seq_given' argument is provided, this
  // form is accepted and the argument is set to 0 unless the sequence part is
  // specified.
  static bool StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                   bool *seq_given);

  // Wrapper for streamGenericParseID() with 'strict' argument set to
  // 0, to be used when - and + are acceptable IDs.
  static bool StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq);

  // Wrapper for streamGenericParseID() with 'strict' argument set to
  // 1, to be used when we want to return an error if the special IDs + or -
  // are provided.
  static bool StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, bool *seq_given);

  // Helper for parsing a stream ID that is a range query interval. When the
  // exclude argument is NULL, streamParseID() is called and the interval
  // is treated as close (inclusive). Otherwise, the exclude argument is set if
  // the interval is open (the "(" prefix) and streamParseStrictID() is
  // called in that case.
  static bool StreamParseIntervalId(const std::string &var, streamID &id, bool *exclude, uint64_t missing_seq);
};

}  // namespace storage
