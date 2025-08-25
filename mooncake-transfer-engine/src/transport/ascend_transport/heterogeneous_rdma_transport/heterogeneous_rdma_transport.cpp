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

#include "transport/ascend_transport/heterogeneous_rdma_transport.h"

namespace mooncake {
HeterogeneousRdmaTransport::HeterogeneousRdmaTransport() {
    transport_ = new RdmaTransport();
}

HeterogeneousRdmaTransport::~HeterogeneousRdmaTransport() { 
    running_ = false;
    delete transport_; 
}

void HeterogeneousRdmaTransport::transfer_Loop() {
    Status s;
    aclrtStream stream;
    int ret = aclrtSetDevice(deviceLogicId_);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice error, ret: " << ret;
    }

    ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtCreateStream error, ret: " << ret;
    }

    while (running_) {
        std::unique_lock<std::mutex> lock(transfer_mutex_);
        if (reqQueues_.empty()) {
            transfer_cond_.wait(lock);
        }

        auto pkg = std::move(taskQueues_.front());
        const auto& task_list = pkg.tasks; 
        taskQueues_.pop();
        lock.unlock();
        if (task_list.empty()) {
            LOG(ERROR) << "HcclTransport: empty transfer request batch";
        }
        int total_length = pkg.total_length;
        if ((offset_ + total_length) >= HUGE_HOST_SIZE) {
            offset_ = 0;
        }
        for (size_t index = 0; index < task_list.size(); ++index) {
            auto &task = *task_list[index];
            auto &request = *task.request;
            request.source = static_cast<char *>(hostAddr_) + offset_;
            offset_ += request.length;
        }

        ret = aclrtMemcpyAsync(static_cast<char *>(hostAddr_) + offset_ - total_length,
                                total_length, hugeDevAddrs[pkg.devId], total_length,
                                ACL_MEMCPY_DEVICE_TO_HOST, stream_);
        if (ret) {
                LOG(ERROR)
                    << "HeterogeneousRdmaTransport: aclrtMemcpyAsync dtoh error, ret: "
                    << ret << ", hostAddr: " << hostAddr_x
                    << ", offset_: " << offset_
                    << ", deviceAddr: " << hugeDevAddrs[devId]
                    << "len: " << total_length;
                return Status::InvalidArgument(
                    "HeterogeneousRdmaTransport: Exceed the limitation of "
                    "capacity, batch id: ");
            }
        s = transport_->submitTransferTask(task_list);
        if (!s.ok()) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: Rdma submitTransferTask error";
        }
    }
}

int HeterogeneousRdmaTransport::install(std::string &local_server_name,
                                        std::shared_ptr<TransferMetadata> meta,
                                        std::shared_ptr<Topology> topo) {
    int ret = 0;
    local_server_name_ = local_server_name;
    running_ = true;ß

    ret = aclrtGetDevice(&deviceLogicId_);
    if (ret) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtGetDevice failed, ret: "
                   << ret;
        return ret;
    }

    LOG(INFO) << "HeterogeneousRdmaTransport: begin to install transport,  "
                 "local_server_name: "
              << local_server_name << "deviceLogicId_: " << deviceLogicId_;

    hostAddr_ = (void *)malloc(HUGE_HOST_SIZE);
    memset(hostAddr_, 0, HUGE_HOST_SIZE);
    if (transport_ == nullptr) {
        LOG(ERROR) << "HeterogeneousRdmaTransport:transport is null";
        return ret;
    }

    devAddr_ = nullptr;
    ret = aclrtMalloc(&devAddr, HUGE_DEVICE_NUM * HUGE_DEVICE_NUM, ACL_MEM_MALLOC_HUGE_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    for (int i = 0; i < HUGE_DEVICE_NUM; i++) {
        hugeDevAddrs.push_back(static_cast<char *>(devAddr_) + i * HUGE_DEVICE_SIZE);
    }

    transferThread_ =
            std::thread(&HcclTransport::transferLoop, this);

    ret = transport_->install(local_server_name_, meta, topo);
    if (ret) {
        LOG(ERROR) << "RdmaTransport install error, ret: " << ret;
        return ret;
    }
    ret = transport_->registerLocalMemory(hostAddr_, HUGE_HOST_SIZE, "cpu",
                                          true, true);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: registerLocalMemory error, ret: "
            << ret;
        return ret;
    }
    return ret;
}

int HeterogeneousRdmaTransport::registerLocalMemory(void *addr, size_t length,
                                                    const std::string &name,
                                                    bool remote_accessible,
                                                    bool update_metadata) {
    aclrtPtrAttributes attributes;
    int ret = aclrtPointerGetAttributes(addr, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return ret;
    }

    if (attributes.location.type == 0) {
        ret = transport_->registerLocalMemory(addr, length, "cpu", true, true);
        if (ret) {
            LOG(ERROR) << "rdma transport registerLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousRdmaTransport::unregisterLocalMemory(void *addr,
                                                      bool update_metadata) {
    aclrtPtrAttributes attributes;
    int ret = aclrtPointerGetAttributes(addr, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return ret;
    }

    if (attributes.location.type == 0) {
        ret = transport_->unregisterLocalMemory(addr, true);
        if (ret) {
            LOG(ERROR) << "rdma transport unregisterLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousRdmaTransport::registerLocalMemoryBatch(
    const std::vector<HeterogeneousRdmaTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    int ret;
    for (auto &buffer : buffer_list) {
        ret = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                  false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport registerLocalMemoryBatch "
                          "error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int HeterogeneousRdmaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    int ret;
    for (auto &addr : addr_list) {
        ret = unregisterLocalMemory(addr, false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport "
                          "unregisterLocalMemoryBatch error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

Status HeterogeneousRdmaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    if (entries.empty()) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: empty transfer request batch";
        return Status::OK();
    }
    std::vector<TransferRequest> new_entries;
    new_entries.resize(entries.size());
    int index = 0;
    aclError ret;
    if (firstSubmit_) {
        ret = aclrtSetDevice(deviceLogicId_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtSetDevice failed ret: "
                << ret;
        }
        ret = aclrtCreateStream(&stream_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtCreateStream error, ret: "
                << ret;
        }
        firstSubmit_ = false;
    }
    aclrtPtrAttributes attributes;
    ret = aclrtPointerGetAttributes(entries[0].source, &attributes);
    if (ret) {
        memcpy_mutex_.unlock();
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: Exceed the limitation of "
            "capacity, batch id: ");
    }

    if (attributes.location.type == 0) {
        return transport_->submitTransfer(batch_id, new_entries);
    }
    memcpy_mutex_.lock();
    int offset = 0;
    for (auto &request : entries) {
        ret = aclrtMemcpyAsync(static_cast<char *>(hostAddr_) + offset_,
                               request.length, devAddr_, request.length,
                               ACL_MEMCPY_DEVICE_TO_HOST, stream_);
        if (ret) {
            memcpy_mutex_.unlock();
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtMemcpyAsync dtoh error, ret: "
                << ret << ", hostAddr: " << hostAddr_
                << ", offset_: " << offset_
                << ", deviceAddr: " << request.source
                << "len: " << request.length;
            return Status::InvalidArgument(
                "HeterogeneousRdmaTransport: Exceed the limitation of "
                "capacity, batch id: ");
        }
        
        new_entries[index] = request;
        new_entries[index].source = static_cast<char *>(hostAddr_) + offset_;
        offset_ += request.length;
        if (offset_ >= HUGE_HOST_SIZE) {
            offset_ = 0;
        }
    }

    ret = aclrtSynchronizeStream(stream_);
    if (ret) {
        memcpy_mutex_.unlock();
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: aclrtSynchronizeStream error, ret: "
            << ret;
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: Exceed the limitation of capacity, "
            "batch id: ");
    }

    memcpy_mutex_.unlock();

    return transport_->submitTransfer(batch_id, new_entries);
}

Status HeterogeneousRdmaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    aclError ret;
    if (task_list.empty()) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: empty transfer task list";
        return Status::OK();
    }

    if (firstSubmit_) {
        ret = aclrtSetDevice(deviceLogicId_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtSetDevice failed ret: "
                << ret;
        }

        ret = aclrtCreateStream(&stream_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtCreateStream error, ret: "
                << ret;
        }
        firstSubmit_ = false;
    }

    auto &task_f = *task_list[0];
    auto &request_f = *task_f.request;
    aclrtPtrAttributes attributes;
    ret = aclrtPointerGetAttributes(request_f.source, &attributes);
    if (ret) {
        memcpy_mutex_.unlock();
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: Exceed the limitation of "
            "capacity, batch id: ");
    }

    if (attributes.location.type == 0) {
        return transport_->submitTransferTask(task_list);
    }

    memcpy_mutex_.lock();
    uint64_t total_length = 0;
    std::vector<TransferTask*> subTasks;
    for (size_t index = 0; index < task_list.size(); ++index) {
        auto &task = *task_list[index];
        auto &request = *task.request;
        total_length += request.length;
        if (total_length >= HUGE_DEVICE_LIMIT) {
            std::unique_lock<std::mutex> lock(transfer_mutex_);
            taskQueues_.emplace(std::move(subTasks), total_length - request.length, devId_);
            lock.unlock();
            transfer_cond_.notify_one();
            subTasks.clear();
            total_length = request.length;
            devId_ = (devId_ + 1) & (HUGE_DEVICE_NUM - 1);
        }
        ret = aclrtMemcpyAsync(static_cast<char *>(hugeDevAddrs[devId_]) + total_length,
                               request.length, request.source, request.length,
                               ACL_MEMCPY_DEVICE_TO_DEVICE, stream_);
        if (ret) {
            memcpy_mutex_.unlock();
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtMemcpyAsync dtod error, ret: "
                << ret << ", hostAddr: " << hostAddr_
                << ", offset_: " << offset_
                << ", deviceAddr: " << request.source
                << ", len: " << request.length;
            return Status::InvalidArgument(
                "HeterogeneousRdmaTransport: Exceed the limitation of "
                "capacity, batch id: ");
        }
        subTasks.push_back(task_list[index]);
    }

    ret = aclrtSynchronizeStream(stream_);
    if (ret) {
        memcpy_mutex_.unlock();
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: aclrtSynchronizeStream error, ret: "
            << ret;
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: Exceed the limitation of capacity, "
            "batch id: ");
    }
    memcpy_mutex_.unlock();

    return Status::OK();
}

Status HeterogeneousRdmaTransport::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus> &status) {
    return transport_->getTransferStatus(batch_id, status);
}

Status HeterogeneousRdmaTransport::getTransferStatus(BatchID batch_id,
                                                     size_t task_id,
                                                     TransferStatus &status) {
    return transport_->getTransferStatus(batch_id, task_id, status);
}

}  // namespace mooncake
