
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <set>
#include <span>
#include <thread>

#include <scq/slotted_cart_queue.hpp>

#include "../concurrent_cross_off_list.hpp"

static constexpr std::chrono::milliseconds wait_time(10);

TEST(multiple_item_cart_enqueue_limit_test, single_producer_single_consumer_all_full_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{3}, scq::cart_count{3}, scq::cart_capacity{2}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {1, value_type{100}},
        {1, value_type{101}}, // full cart 1
        {1, value_type{102}},
        {1, value_type{103}}, // full cart 2
        {2, value_type{200}},
        {2, value_type{201}}, // full cart 3
        {2, value_type{202}},
        {2, value_type{203}} // full cart 4
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count, &enqueue_count_cv]
    {
        queue.enqueue(scq::slot_id{1}, value_type{100});
        queue.enqueue(scq::slot_id{1}, value_type{101}); // full cart 1
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{102});
        queue.enqueue(scq::slot_id{1}, value_type{103}); // full cart 2
        ++enqueue_count;
        queue.enqueue(scq::slot_id{2}, value_type{200});
        queue.enqueue(scq::slot_id{2}, value_type{201}); // full cart 3
        ++enqueue_count;

        enqueue_count_cv.notify_one();

        // this should block
        queue.enqueue(scq::slot_id{2}, value_type{202});
        queue.enqueue(scq::slot_id{2}, value_type{203}); // full cart 4
        ++enqueue_count;
    }};

    // wait until at least 3 carts are full
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 3;
        });
    }

    // queue should block on 4th insertion (max cart_count is just 3)
    // i.e. enqueue_count stays at 3
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 3);

    // release one full cart to allow enqueue to continue
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.second.size(), 2u);

        for (auto && value: cart_data.second)
        {
            EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
        }
    }

    enqueue_thread.join();
    queue.close();

    EXPECT_EQ(enqueue_count.load(), 4);

    for (int i = 0; i < 3; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.second.size(), 2u);

        for (auto && value: cart_data.second)
        {
            EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
        }
    }

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(multiple_item_cart_enqueue_limit_test, single_producer_single_consumer_mixed_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{3}, scq::cart_count{3}, scq::cart_capacity{2}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {0, value_type{001}}, // non-full cart 1
        {1, value_type{100}}, // non-full cart 2
        {2, value_type{200}},
        {2, value_type{201}}, // full cart 2
        {2, value_type{202}},
        {2, value_type{203}} // full cart 3
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count, &enqueue_count_cv]
    {
        queue.enqueue(scq::slot_id{0}, value_type{001}); // non-full cart 1 (will be released after close)
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{100}); // non-full cart 2 (will be released after close)
        ++enqueue_count;
        queue.enqueue(scq::slot_id{2}, value_type{200});
        queue.enqueue(scq::slot_id{2}, value_type{201}); // full cart 1
        ++enqueue_count;

        enqueue_count_cv.notify_one();

        // this should block, because all 3 empty-carts were take: 2 carts are in fill-mode and 1 cart is full
        queue.enqueue(scq::slot_id{2}, value_type{202});
        queue.enqueue(scq::slot_id{2}, value_type{203}); // full cart 2
        ++enqueue_count;
    }};

    // wait until at least 3 carts are non-emtpy
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 3;
        });
    }

    // queue should block on 4th insertion (max cart_count is just 3)
    // i.e. enqueue_count stays at 3
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 3);

    size_t full_cart_count{};
    size_t half_filled_cart_count{};

    // release one full cart to allow enqueue to continue
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.second.size(), 2u);
        full_cart_count += cart_data.second.size() == 2;

        for (auto && value: cart_data.second)
        {
            EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
        }
    }

    enqueue_thread.join();
    queue.close();

    EXPECT_EQ(enqueue_count.load(), 4);

    for (int i = 0; i < 3; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_GE(cart_data.second.size(), 1u);
        EXPECT_LE(cart_data.second.size(), 2u);

        half_filled_cart_count += cart_data.second.size() == 1;
        full_cart_count += cart_data.second.size() == 2;

        for (auto && value: cart_data.second)
        {
            EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
        }
    }

    EXPECT_EQ(half_filled_cart_count, 2);
    EXPECT_EQ(full_cart_count, 2);

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(multiple_item_cart_enqueue_limit_test, single_producer_multiple_consumer_all_full_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{3}, scq::cart_count{3}, scq::cart_capacity{2}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {0, value_type{001}},
        {0, value_type{002}}, // full cart 1
        {1, value_type{100}},
        {1, value_type{101}}, // full cart 2
        {1, value_type{102}},
        {1, value_type{103}}, // full cart 3
        {2, value_type{200}},
        {2, value_type{201}}, // full cart 4
        {2, value_type{202}},
        {2, value_type{203}} // full cart 5
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count, &enqueue_count_cv]
    {
        queue.enqueue(scq::slot_id{0}, value_type{001}); // slot 0: [1/2]
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{100}); // slot 1: [1/2]
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{101}); // slot 1: [2/2]
        queue.enqueue(scq::slot_id{2}, value_type{200}); // slot 2: [1/2]
        ++enqueue_count;

        enqueue_count_cv.notify_one();

        // this should block
        queue.enqueue(scq::slot_id{1}, value_type{102}); // slot 1: [1/2]
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{103}); // slot 1: [2/2]

        queue.enqueue(scq::slot_id{0}, value_type{002}); // slot 0: [2/2]

        queue.enqueue(scq::slot_id{2}, value_type{201}); // slot 2: [2/2]
        queue.enqueue(scq::slot_id{2}, value_type{202}); // slot 2: [1/2]
        queue.enqueue(scq::slot_id{2}, value_type{203}); // slot 2: [2/2]
        ++enqueue_count;
    }};

    // wait until at least 3 carts are non-empty
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 3;
        });
    }

    // queue should block on 4th insertion (max cart_count is just 3)
    // i.e. enqueue_count stays at 3
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 3);

    // concurrently dequeue
    std::vector<std::thread> dequeue_threads(5);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        return std::thread([&queue, &expected]
        {
            scq::cart<value_type> cart = queue.dequeue();
            EXPECT_TRUE(cart.valid());
            std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

            EXPECT_EQ(cart_data.second.size(), 2u);

            for (auto && value: cart_data.second)
            {
                EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
            }
        });
    });

    enqueue_thread.join();

    EXPECT_EQ(enqueue_count.load(), 5);

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(multiple_item_cart_enqueue_limit_test, single_producer_multiple_consumer_mixed_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{3}, scq::cart_count{3}, scq::cart_capacity{2}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {0, value_type{001}}, // non-full cart 1
        {1, value_type{100}},
        {1, value_type{101}}, // full cart 1
        {1, value_type{102}}, // non-full cart 2
        {2, value_type{200}},
        {2, value_type{201}}, // full cart 2
        {2, value_type{202}},
        {2, value_type{203}} // full cart 3
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count, &enqueue_count_cv]
    {
        queue.enqueue(scq::slot_id{0}, value_type{001}); // slot 0: [1/2]
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{100}); // slot 1: [1/2]
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{101}); // slot 1: [2/2]
        queue.enqueue(scq::slot_id{2}, value_type{200}); // slot 2: [1/2]
        ++enqueue_count;

        enqueue_count_cv.notify_one();

        // this should block, because the 3 empty carts are taken
        queue.enqueue(scq::slot_id{1}, value_type{102}); // slot 1: [1/2]
        ++enqueue_count;

        queue.enqueue(scq::slot_id{2}, value_type{201}); // slot 2: [2/2]
        queue.enqueue(scq::slot_id{2}, value_type{202}); // slot 2: [1/2]
        queue.enqueue(scq::slot_id{2}, value_type{203}); // slot 2: [2/2]
        ++enqueue_count;
    }};

    // wait until at least 3 carts are non-empty
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 3;
        });
    }

    // queue should block on 4th insertion (max cart_count is just 3)
    // i.e. enqueue_count stays at 3
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 3);

    std::atomic_size_t full_cart_count{};
    std::atomic_size_t half_filled_cart_count{};

    // concurrently dequeue
    std::vector<std::thread> dequeue_threads(5);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        return std::thread([&queue, &expected, &full_cart_count, &half_filled_cart_count]
        {
            scq::cart<value_type> cart = queue.dequeue();
            EXPECT_TRUE(cart.valid());
            std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

            EXPECT_GE(cart_data.second.size(), 1u);
            EXPECT_LE(cart_data.second.size(), 2u);

            half_filled_cart_count += cart_data.second.size() == 1;
            full_cart_count += cart_data.second.size() == 2;

            for (auto && value: cart_data.second)
            {
                EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
            }
        });
    });

    enqueue_thread.join();
    queue.close();

    EXPECT_EQ(enqueue_count.load(), 5);

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();

    EXPECT_EQ(full_cart_count.load(), 3);
    EXPECT_EQ(half_filled_cart_count.load(), 2);

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(multiple_item_cart_enqueue_limit_test, multiple_producer_single_consumer_all_full_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{3}, scq::cart_count{3}, scq::cart_capacity{2}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {0, value_type{001}},
        {0, value_type{002}}, // full cart 1
        {1, value_type{100}},
        {1, value_type{101}}, // full cart 2
        {1, value_type{102}},
        {1, value_type{103}}, // full cart 3
        {2, value_type{200}},
        {2, value_type{201}} // full cart 4
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    // initialise 8 producing threads
    std::vector<std::thread> enqueue_threads(8);
    std::generate(enqueue_threads.begin(), enqueue_threads.end(), [&]()
    {
        static size_t thread_id = 0;
        return std::thread([thread_id = thread_id++, &queue, &enqueue_count, &enqueue_count_cv]
        {
            std::this_thread::sleep_for(thread_id * wait_time);

            switch (thread_id)
            {
                case 0:
                    queue.enqueue(scq::slot_id{0}, value_type{001}); // cart 1 [1/2]
                    break;
                case 1:
                    queue.enqueue(scq::slot_id{0}, value_type{002}); // cart 1 [2/2]
                    break;
                case 2:
                    queue.enqueue(scq::slot_id{1}, value_type{100}); // cart 2 [1/2]
                    break;
                case 3:
                    queue.enqueue(scq::slot_id{1}, value_type{101}); // cart 2 [2/2]
                    break;
                case 4:
                    queue.enqueue(scq::slot_id{2}, value_type{200}); // cart 3 [1/2]
                    break;
                case 5:
                    queue.enqueue(scq::slot_id{1}, value_type{103}); // cart 4 [1/2] wait after 201 is inserted
                    break;
                case 6:
                    queue.enqueue(scq::slot_id{1}, value_type{102}); // cart 4 [2/2] wait after 201 is inserted
                    break;
                case 7:
                    queue.enqueue(scq::slot_id{2}, value_type{201}); // cart 3 [2/2]
                    break;
            }

            ++enqueue_count;
            enqueue_count_cv.notify_one();
        });
    });

    // wait until at least 6 elements are inserted
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 6;
        });
    }

    // queue should block after 6 inserts
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 6);

    // release one cart to allow enqueue to continue
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.second.size(), 2u);

        for (auto && value: cart_data.second)
        {
            EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
        }
    }

    for (auto && enqueue_thread: enqueue_threads)
        enqueue_thread.join();

    EXPECT_EQ(enqueue_count.load(), 8);

    for (int i = 0; i < 3; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.second.size(), 2u);

        for (auto && value: cart_data.second)
        {
            EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
        }
    }

    // all results seen
    EXPECT_TRUE(expected.empty());
}

