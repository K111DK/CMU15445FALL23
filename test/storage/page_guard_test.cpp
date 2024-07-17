//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {
template <typename... Args>
auto LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> threads;
  for (uint64_t thread_tier = 0; thread_tier < num_threads; ++thread_tier) {
    threads.push_back(std::thread(args..., thread_tier));
  }
  for (uint64_t thread_tier = 0; thread_tier < num_threads; ++thread_tier) {
    threads[thread_tier].join();
  }
}

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);
  page_id_t page_id_temp;
  auto page0_guard = bpm->NewPageGuarded(&page_id_temp);
  std::thread th1([&]() {
    auto page_guard = bpm->FetchPageWrite(page_id_temp);
    auto basic_guard = bpm->FetchPageBasic(page_id_temp);
    WritePageGuard page0copy_guard = std::move(page_guard);
  });
  std::thread th2([&]() {
    auto page_guard = bpm->FetchPageRead(page_id_temp);
    auto basic_guard = bpm->FetchPageBasic(page_id_temp);
    ReadPageGuard page1copy_guard = std::move(page_guard);
  });
  th1.join();
  th2.join();
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
