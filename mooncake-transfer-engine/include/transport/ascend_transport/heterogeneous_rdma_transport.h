// Copyright 2025 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef HETEROGENEOUS_RDMA_TRANSPORT_H_
#define HETEROGENEOUS_RDMA_TRANSPORT_H_

#include "transport/rdma_transport/rdma_transport.h"
#include "acl/acl.h"
#include <atomic>
#include <condition_variable>

#define HUGE_HOST_SIZE 3ULL * 1024 * 1024 * 1024
#define HUGE_DEVICE_SIZE 8 * 1024 * 1024
#define HUGE_DEVICE_NUM 4

namespace mooncake {

class HeterogeneousRdmaTransport : public Transport {
   public:
    HeterogeneousRdmaTransport();

    ~HeterogeneousRdmaTransport();

    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    const char *getName() const override { return "ascend"; }

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    // TRANSFER

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus> &status);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

   private:
    void transfer_Loop();

   private:
    struct TaskPackage {
        std::vector<TransferTask *> tasks;
        uint64_t total_length;
        uint64_t devId;

        TaskPackage(TaskPackage &&) = default;
        TaskPackage &operator=(TaskPackage &&) = default;

        TaskPackage(const TaskPackage &) = delete;
        TaskPackage &operator=(const TaskPackage &) = delete;

        TaskPackage(std::vector<TransferTask *> taskList, uint64_t len,
                    uint64_t id)
            : tasks(std::move(taskList)), total_length(len), devId(id) {}
    };
    bool running_ = false;
    RdmaTransport *transport_ = nullptr;
    aclrtStream stream_;
    void *hostAddr_ = NULL;
    void *devAddr_ = NULL;
    std::vector<void *> hugeDevAddrs;
    int deviceLogicId_;
    bool firstSubmit_ = true;
    std::mutex memcpy_mutex_;
    uint64_t offset_ = 0;
    std::thread transferThread_;
    std::queue<TaskPackage> taskQueues_;
    std::mutex transfer_mutex_;
    std::condition_variable transfer_cond_;
    std::atomic<int> task_counter_;
    int devId_ = 0;
    std::array<bool, HUGE_DEVICE_NUM> mem_blocks = {false, false, false, false};
    std::mutex dev_mtx_;
    std::condition_variable dev_cv_;
};

using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;

}  // namespace mooncake

#endif  // HETEROGENEOUS_RDMA_TRANSPORT_H_