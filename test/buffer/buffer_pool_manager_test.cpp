//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <limits>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

//// NOLINTNEXTLINE
//// Check whether pages containing terminal characters can be recovered
// TEST(BufferPoolManagerTest, BinaryDataTest) {
//   const std::string db_name = "test.db";
//   const size_t buffer_pool_size = 10;
//   const size_t k = 5;
//
//   std::random_device r;
//   std::default_random_engine rng(r());
//
//   constexpr int lower_bound = static_cast<int>(std::numeric_limits<char>::min());
//   constexpr int upper_bound = static_cast<int>(std::numeric_limits<char>::max());
//   // No matter if `char` is signed or unsigned by default, this constraint must be met
//   static_assert(upper_bound - lower_bound == 255);
//   std::uniform_int_distribution<int> uniform_dist(lower_bound, upper_bound);
//
//   auto *disk_manager = new DiskManager(db_name);
//   auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);
//
//   page_id_t page_id_temp;
//   auto *page0 = bpm->NewPage(&page_id_temp);
//
//   // Scenario: The buffer pool is empty. We should be able to create a new page.
//   ASSERT_NE(nullptr, page0);
//   EXPECT_EQ(0, page_id_temp);
//   bpm->UnpinPage(page_id_temp, true, AccessType::Unknown);
//   // BUSTUB_ASSERT(page0->GetPinCount() == 0, "Pin error");
//   std::vector<std::thread> threads;
//   const size_t n_threads = 16;
//   for (size_t i = 0; i < n_threads; ++i) {
//     threads.emplace_back([&bpm, page_id_temp] {
//       for (auto i = 0; i < 1000; ++i) {
//         bpm->FlushPage(page_id_temp);
//       }
//     });
//   }
//   for (auto &thread : threads) {
//     thread.join();
//   }
//   // BUSTUB_ASSERT(page0->GetPinCount() == 0, "Pin cnt error");
//   //   char random_binary_data[BUSTUB_PAGE_SIZE];
//   //   // Generate random binary data
//   //   for (char &i : random_binary_data) {
//   //     i = static_cast<char>(uniform_dist(rng));
//   //   }
//   //
//   //   // Insert terminal characters both in the middle and at end
//   //   random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
//   //   random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';
//   //
//   //   // Scenario: Once we have a page, we should be able to read and write content.
//   //   std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
//   //   EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
//   //
//   //   // Scenario: We should be able to create new pages until we fill up the buffer pool.
//   //   for (size_t i = 1; i < buffer_pool_size; ++i) {
//   //     EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
//   //   }
//   //
//   //   // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
//   //   for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
//   //     EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
//   //   }
//   //
//   //   // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages
//   //   for (int i = 0; i < 5; ++i) {
//   //     EXPECT_EQ(true, bpm->UnpinPage(i, true));
//   //     bpm->FlushPage(i);
//   //   }
//   //   for (int i = 0; i < 5; ++i) {
//   //     EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
//   //     // Unpin the page here to allow future fetching
//   //     bpm->UnpinPage(page_id_temp, false);
//   //   }
//   //
//   //   // Scenario: We should be able to fetch the data we wrote a while ago.
//   //   page0 = bpm->FetchPage(0);
//   //   ASSERT_NE(nullptr, page0);
//   //   EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
//   //   EXPECT_EQ(true, bpm->UnpinPage(0, true));
//
//   // Shutdown the disk manager and remove the temporary file we created.
//   disk_manager->ShutDown();
//   remove("test.db");
//
//   delete bpm;
//   delete disk_manager;
// }

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 100;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);
  size_t n_threads = 16;
  std::vector<std::thread> threads;
  for (size_t i = 0; i < n_threads; ++i) {
    threads.emplace_back([&bpm] {
      for (auto i = 0; i < 1000; ++i) {
        page_id_t page_id_temp;
        auto *page0 = bpm->NewPage(&page_id_temp);
        if (page0 == nullptr) {
          continue;
        }
        bpm->UnpinPage(page_id_temp, true);
      }
      // bpm->FlushAllPages();
    });
    threads.emplace_back([&bpm] {
      for (auto i = 0; i < 1000; ++i) {
        auto page0 = bpm->FetchPage(i);
        if (page0 == nullptr) {
          continue;
        }
        bpm->UnpinPage(i, true);
        bpm->FlushPage(i);
      }
      // bpm->FlushAllPages();
    });
  }  //  std::vector<std::thread> delete_threads;
     //  size_t i = 0;
     //  for (i = 0; i < n_threads; ++i) {
     //    delete_threads.emplace_back([=, &bpm] {
     //      for (auto j = 0; j < 100; ++j) {
     //        page_id_t page_id_temp = i * 100 + j;
     //        auto page = bpm->FetchPage(page_id_temp);
     //        bpm->UnpinPage(page->GetPageId(), true);
     //        bpm->DeletePage(page->GetPageId());
     //      }
     //    });
     //  }
     //  for (auto &thread : delete_threads) {
     //    thread.join();
     //  }

  for (auto &thread : threads) {
    thread.join();
  }

  //  std::vector<std::thread> delete_threads;
  //  size_t i = 0;
  //  for (i = 0; i < n_threads; ++i) {
  //    delete_threads.emplace_back([=, &bpm] {
  //      for (auto j = 0; j < 100; ++j) {
  //        page_id_t page_id_temp = i * 100 + j;
  //        auto page = bpm->FetchPage(page_id_temp);
  //        bpm->UnpinPage(page->GetPageId(), true);
  //        bpm->DeletePage(page->GetPageId());
  //      }
  //    });
  //  }
  //  for (auto &thread : delete_threads) {
  //    thread.join();
  //  }

  //  page_id_t page_id_temp;
  //  auto *page0 = bpm->NewPage(&page_id_temp);
  //
  //  // Scenario: The buffer pool is empty. We should be able to create a new page.
  //  ASSERT_NE(nullptr, page0);
  //  EXPECT_EQ(0, page_id_temp);
  //
  //  // Scenario: Once we have a page, we should be able to read and write content.
  //  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  //  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));
  //
  //  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  //  for (size_t i = 1; i < buffer_pool_size; ++i) {
  //    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  //  }
  //
  //  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  //  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
  //    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  //  }
  //
  //  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  //  // there would still be one buffer page left for reading page 0.
  //  for (int i = 0; i < 5; ++i) {
  //    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  //  }
  //  for (int i = 0; i < 4; ++i) {
  //    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  //  }
  //
  //  // Scenario: We should be able to fetch the data we wrote a while ago.
  //  page0 = bpm->FetchPage(0);
  //  ASSERT_NE(nullptr, page0);
  //  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));
  //
  //  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  //  // now be pinned. Fetching page 0 again should fail.
  //  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  //  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  //  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
