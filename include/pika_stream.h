
#ifndef PIKA_STREAM_H_
#define PIKA_STREAM_H_

#include <bits/stdint-intn.h>
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>
#include "gflags/gflags_declare.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "include/pika_stream_base.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "storage/storage.h"

/*
 * stream
 */
class XAddCmd : public Cmd {
 public:
  XAddCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XAddCmd(*this); }

 private:
  std::string key_;
  StreamAddTrimArgs args_;
  int field_pos_{0};

  void DoInitial() override;
  inline void GenerateStreamIDOrReply(const StreamMetaValue& stream_meta);
  inline std::string SerializeMessage(const std::vector<std::string>& field_values, int field_pos) {
    assert(field_values.size() - field_pos >= 2 && (field_values.size() - field_pos) % 2 == 0);

    std::string message;
    // count the size of serizlized message
    size_t size = 0;
    for (int i = field_pos; i < field_values.size(); i++) {
      size += field_values[i].size() + sizeof(size_t);
    }
    message.reserve(size);

    // serialize message
    for (int i = field_pos; i < field_values.size(); i++) {
      size_t len = field_values[i].size();
      message.append(reinterpret_cast<const char*>(&len), sizeof(len));
      message.append(field_values[i]);
    }

    return message;
  }
};

class XDelCmd : public Cmd {
 public:
  XDelCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XDelCmd(*this); }

 private:
  std::string key_;
  std::vector<streamID> ids_;

  void DoInitial() override;
  void Clear() override { ids_.clear(); }
  inline void SetFirstOrLastIDOrReply(StreamMetaValue& stream_meta, const std::shared_ptr<Slot>& slot,
                                      bool is_set_first);
  inline void SetFirstIDOrReply(StreamMetaValue& stream_meta, const std::shared_ptr<Slot>& slot);
  inline void SetLastIDOrReply(StreamMetaValue& stream_meta, const std::shared_ptr<Slot>& slot);
};

class XReadCmd : public Cmd {
 public:
  XReadCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XReadCmd(*this); }

 private:
  StreamReadGroupReadArgs args_;

  void DoInitial() override;
  void Clear() override {
    args_.unparsed_ids.clear();
    args_.keys.clear();
  }
};

class XRangeCmd : public Cmd {
 public:
  XRangeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XRangeCmd(*this); }

 protected:
  std::string key_;
  streamID start_sid;
  streamID end_sid;
  int32_t count_{INT32_MAX};
  bool start_ex_{false};
  bool end_ex_{false};

  void DoInitial() override;
};

class XRevrangeCmd : public XRangeCmd {
 public:
  XRevrangeCmd(const std::string& name, int arity, uint16_t flag) : XRangeCmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XRevrangeCmd(*this); }
};

class XLenCmd : public Cmd {
 public:
  XLenCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XLenCmd(*this); }

 private:
  std::string key_;

  void DoInitial() override;
};

class XTrimCmd : public Cmd {
 public:
  XTrimCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XTrimCmd(*this); }

 private:
  std::string key_;
  StreamAddTrimArgs args_;

  void DoInitial() override;
};

class XInfoCmd : public Cmd {
 public:
  XInfoCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XInfoCmd(*this); }

 private:
  std::string key_;
  std::string cgroupname_;
  std::string consumername_;
  std::string subcmd_;
  uint64_t count_{0};
  bool is_full_{false};

  void DoInitial() override;
  void StreamInfo(std::shared_ptr<Slot>& slot);
  void GroupsInfo(std::shared_ptr<Slot>& slot);
  void ConsumersInfo(std::shared_ptr<Slot>& slot);
};

class XReadGroupCmd : public Cmd {
 public:
  XReadGroupCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XReadGroupCmd(*this); }

 private:
  StreamReadGroupReadArgs args_;

  // return ok if consumer meta exists or create a new one
  // @consumer_meta: used to return the consumer meta
  rocksdb::Status GetOrCreateConsumer(treeID consumer_tid, std::string& consumername, const std::shared_ptr<Slot>& slot,
                                      StreamConsumerMetaValue& consumer_meta);
  void DoInitial() override;
  void Clear() override {
    args_.unparsed_ids.clear();
    args_.keys.clear();
  }
};

class XGroupCmd : public Cmd {
 public:
  XGroupCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XGroupCmd(*this); }

 private:
  // create a consumer group, initialize the pel and consumers
  void Create(const std::shared_ptr<Slot>& slot = nullptr);
  void CreateConsumer(const std::shared_ptr<Slot>& slot = nullptr);
  void DeleteConsumer(const std::shared_ptr<Slot>& slot = nullptr);
  void Destroy(const std::shared_ptr<Slot>& slot = nullptr);

  void Help(const std::shared_ptr<Slot>& slot = nullptr);

 private:
  // XGROUP common options
  std::string subcmd_;
  std::string key_;
  std::string cgroupname_;
  streamID sid_;
  StreamMetaValue stream_meta_;

  // CREATE and SETID options
  uint64_t entries_read_{0};
  bool id_given_{false};

  // CREATE options
  bool mkstream_{false};

  // CREATECONSUMER and DELCONSUMER options
  std::string consumername;

  void DoInitial() override;
  // void Clear() override { .clear(); }
  static CmdRes GenerateStreamID(const StreamMetaValue& stream_meta, const StreamAddTrimArgs& args_, streamID& id);
};

class XAckCmd : public Cmd {
 public:
  XAckCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XAckCmd(*this); }

 private:
  std::string key_;
  std::string cgroup_name_;
  std::vector<std::string> ids_;

  void Clear() override { ids_.clear(); }
  void DoInitial() override;
};

class XClaimCmd : public Cmd {
 public:
  XClaimCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override { return {key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XClaimCmd(*this); }

 private:
  std::string key_;
  std::string cgroup_name_;
  std::string consumer_name_;
  mstime_t min_idle_time_;
  mstime_t parse_option_time_;

  // optional
  std::vector<streamID> specified_ids_;
  mstime_t deliverytime = 0;
  uint64_t retrycount = 0;
  bool deliverytime_flag = false;
  bool retrycount_flag = false;
  bool force = false;
  bool justid = false;
  streamID last_id = {0, 0};

  // save results
  std::vector<std::string> results;

  void DoInitial() override;
  void Clear() override {
    specified_ids_.clear();
    results.clear();
  }
};

#endif  //  PIKA_STREAM_H_
