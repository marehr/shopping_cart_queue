
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <set>
#include <span>
#include <thread>

#include <scq/slotted_cart_queue.hpp>

#include "../concurrent_cross_off_list.hpp"

static constexpr std::chrono::milliseconds wait_time(10);

TEST(single_item_cart_enqueue_limit_test, single_producer_single_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}},
        {2, value_type{201}}
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count, &enqueue_count_cv]
    {
        queue.enqueue(scq::slot_id{1}, value_type{100});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{101});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{102});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{103});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{2}, value_type{200});
        ++enqueue_count;

        enqueue_count_cv.notify_one();

        // this should block
        queue.enqueue(scq::slot_id{2}, value_type{201});
        ++enqueue_count;
    }};

    // wait until at least 5 carts are full
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 5;
        });
    }

    // queue should block on 6th insertion (max cart_count is just 5)
    // i.e. enqueue_count stays at 5
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 5);

    // release one cart to allow enqueue to continue
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_TRUE(expected.cross_off({
            std::get<0>(cart_data).slot_id,
            std::get<1>(cart_data)[0]
        }));
    }

    enqueue_thread.join();

    EXPECT_EQ(enqueue_count.load(), 6);

    for (int i = 0; i < 5; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_TRUE(expected.cross_off({
            std::get<0>(cart_data).slot_id,
            std::get<1>(cart_data)[0]
        }));
    }

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(single_item_cart_enqueue_limit_test, single_producer_multiple_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}},
        {2, value_type{201}}
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count, &enqueue_count_cv]
    {
        queue.enqueue(scq::slot_id{1}, value_type{100});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{101});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{102});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{103});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{2}, value_type{200});
        ++enqueue_count;

        enqueue_count_cv.notify_one();

        // this should block
        queue.enqueue(scq::slot_id{2}, value_type{201});
        ++enqueue_count;
    }};

    // wait until at least 5 carts are full
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 5;
        });
    }

    // queue should block on 6th insertion (max cart_count is just 5)
    // i.e. enqueue_count stays at 5
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 5);

    // concurrently dequeue
    std::vector<std::thread> dequeue_threads(6);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        return std::thread([&queue, &expected]
        {
            scq::cart<value_type> cart = queue.dequeue();
            EXPECT_TRUE(cart.valid());
            std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

            EXPECT_TRUE(expected.cross_off({
                std::get<0>(cart_data).slot_id,
                std::get<1>(cart_data)[0]
            }));
        });
    });

    enqueue_thread.join();

    EXPECT_EQ(enqueue_count.load(), 6);

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(single_item_cart_enqueue_limit_test, multiple_producer_single_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}},
        {2, value_type{201}}
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    // initialise 6 producing threads
    std::vector<std::thread> enqueue_threads(6);
    std::generate(enqueue_threads.begin(), enqueue_threads.end(), [&]()
    {
        static size_t thread_id = 0;
        return std::thread([thread_id = thread_id++, &queue, &enqueue_count, &enqueue_count_cv]
        {
            std::this_thread::sleep_for(thread_id * wait_time);

            switch (thread_id)
            {
                case 0:
                    queue.enqueue(scq::slot_id{1}, value_type{100});
                    break;
                case 1:
                    queue.enqueue(scq::slot_id{1}, value_type{101});
                    break;
                case 2:
                    queue.enqueue(scq::slot_id{2}, value_type{200});
                    break;
                case 3:
                    queue.enqueue(scq::slot_id{1}, value_type{103});
                    break;
                case 4:
                    queue.enqueue(scq::slot_id{1}, value_type{102});
                    break;
                case 5:
                    queue.enqueue(scq::slot_id{2}, value_type{201});
                    break;
            }

            ++enqueue_count;
            enqueue_count_cv.notify_one();
        });
    });

    // wait until at least 5 carts are full
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 5;
        });
    }

    // queue should block on 6th insertion (max cart_count is just 5)
    // i.e. enqueue_count stays at 5
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 5);

    // release one cart to allow enqueue to continue
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_TRUE(expected.cross_off({
            std::get<0>(cart_data).slot_id,
            std::get<1>(cart_data)[0]
        }));
    }

    for (auto && enqueue_thread: enqueue_threads)
        enqueue_thread.join();

    EXPECT_EQ(enqueue_count.load(), 6);

    for (int i = 0; i < 5; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_TRUE(expected.cross_off({
            std::get<0>(cart_data).slot_id,
            std::get<1>(cart_data)[0]
        }));
    }

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(single_item_cart_enqueue_limit_test, multiple_producer_multiple_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}},
        {2, value_type{201}}
    };

    std::mutex enqueue_count_mutex{};
    std::condition_variable enqueue_count_cv{};
    std::atomic_size_t enqueue_count{};

    // initialise 6 producing threads
    std::vector<std::thread> enqueue_threads(6);
    std::generate(enqueue_threads.begin(), enqueue_threads.end(), [&]()
    {
        static size_t thread_id = 0;
        return std::thread([thread_id = thread_id++, &queue, &enqueue_count, &enqueue_count_cv]
        {
            switch (thread_id)
            {
                case 0:
                    queue.enqueue(scq::slot_id{1}, value_type{100});
                    break;
                case 1:
                    queue.enqueue(scq::slot_id{1}, value_type{101});
                    break;
                case 2:
                    queue.enqueue(scq::slot_id{2}, value_type{200});
                    break;
                case 3:
                    queue.enqueue(scq::slot_id{1}, value_type{103});
                    break;
                case 4:
                    queue.enqueue(scq::slot_id{1}, value_type{102});
                    break;
                case 5:
                    queue.enqueue(scq::slot_id{2}, value_type{201});
                    break;
            }

            ++enqueue_count;
            enqueue_count_cv.notify_one();
        });
    });

    // wait until at least 5 carts are full
    {
        std::unique_lock<std::mutex> enqueue_count_lock(enqueue_count_mutex);
        enqueue_count_cv.wait(enqueue_count_lock, [&]
        {
            return enqueue_count.load() >= 5;
        });
    }

    // queue should block on 6th insertion (max cart_count is just 5)
    // i.e. enqueue_count stays at 5
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 5);

    // concurrently dequeue
    std::vector<std::thread> dequeue_threads(6);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        return std::thread([&queue, &expected]
        {
            scq::cart<value_type> cart = queue.dequeue();
            EXPECT_TRUE(cart.valid());
            std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

            EXPECT_TRUE(expected.cross_off({
                std::get<0>(cart_data).slot_id,
                std::get<1>(cart_data)[0]
            }));
        });
    });

    for (auto && enqueue_thread: enqueue_threads)
        enqueue_thread.join();

    EXPECT_EQ(enqueue_count.load(), 6);

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();

    // all results seen
    EXPECT_TRUE(expected.empty());
}
