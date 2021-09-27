
#include <gtest/gtest.h>

#include <chrono>
#include <set>
#include <thread>

#include <scq/slotted_cart_queue.hpp>

#include "../atomic_count.hpp"
#include "../concurrent_cross_off_list.hpp"

static constexpr std::chrono::milliseconds wait_time(10);

TEST(multiple_item_cart_close_queue, no_producer_no_consumer_close_is_non_blocking)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    queue.close();
}

TEST(multiple_item_cart_close_queue, single_producer_no_consumer_enqueue_after_close)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101}); // one full cart

    queue.enqueue(scq::slot_id{1}, value_type{102}); // one half-filled cart

    queue.close();

    EXPECT_THROW(queue.enqueue(scq::slot_id{2}, value_type{200}), std::overflow_error);
}

TEST(multiple_item_cart_close_queue, single_producer_no_consumer_release_blocking_enqueue_when_close)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    // count number of enqueues
    atomic_count enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count]
    {
        queue.enqueue(scq::slot_id{1}, value_type{100});
        queue.enqueue(scq::slot_id{1}, value_type{101});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{1}, value_type{102});
        queue.enqueue(scq::slot_id{1}, value_type{103});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{2}, value_type{200});
        queue.enqueue(scq::slot_id{2}, value_type{201});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{2}, value_type{202});
        queue.enqueue(scq::slot_id{2}, value_type{203});
        ++enqueue_count;
        queue.enqueue(scq::slot_id{3}, value_type{300});
        queue.enqueue(scq::slot_id{3}, value_type{301});
        ++enqueue_count;

        // this should block since 5 carts are full and not dequeued yet
        EXPECT_THROW(queue.enqueue(scq::slot_id{2}, value_type{201}), std::overflow_error);
        ++enqueue_count;
    }};

    // barrier: wait until at least 5 carts are full
    enqueue_count.wait_at_least(5);

    // queue should block on 6th insertion (max cart_count is just 5)
    // i.e. enqueue_count stays at 5
    // we wait to give the enqueue_thread a chance to enqueue the 6th element (which it shouldn't be able)
    std::this_thread::sleep_for(wait_time);
    EXPECT_EQ(enqueue_count.load(), 5);

    // we close the queue which will throw on the 6th element
    queue.close();

    enqueue_thread.join();

    EXPECT_EQ(enqueue_count.load(), 6);
}

TEST(multiple_item_cart_close_queue, multiple_producer_no_consumer_enqueue_after_close)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    // count number of enqueues
    atomic_count enqueue_count{};

    // initialise 5 producing threads
    std::vector<std::thread> enqueue_threads(5);
    std::generate(enqueue_threads.begin(), enqueue_threads.end(), [&]()
    {
        static size_t thread_id = 0;
        return std::thread([thread_id = thread_id++, &queue, &enqueue_count]
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
            }

            ++enqueue_count;

            // barrier: wait until queue was closed (>= 6 means queue was closed)
            enqueue_count.wait_at_least(6);

            // queue should already be closed
            EXPECT_THROW(queue.enqueue(scq::slot_id{0}, value_type{0}), std::overflow_error);
        });
    });

    // barrier: wait until all elements are added
    enqueue_count.wait_at_least(5);

    queue.close();

    ++enqueue_count; // signal that queue is closed

    for (auto && enqueue_thread: enqueue_threads)
        enqueue_thread.join();
}

TEST(multiple_item_cart_close_queue, no_producer_single_consumer_dequeue_after_close)
{
    using value_type = int;

    // close first then dequeue.

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    queue.close();

    // should be non-blocking if queue was closed (without close it would be blocking)
    scq::cart_future<value_type> cart = queue.dequeue();

    EXPECT_FALSE(cart.valid());

    EXPECT_THROW(cart.get(), std::future_error);
}

TEST(multiple_item_cart_close_queue, no_producer_single_consumer_release_blocking_dequeue_when_close)
{
    using value_type = int;

    // dequeue first then close.

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    std::thread dequeue_thread{[&queue]
    {
        // should be blocking if queue was not yet closed
        scq::cart_future<value_type> cart = queue.dequeue();

        EXPECT_FALSE(cart.valid());

        EXPECT_THROW(cart.get(), std::future_error);
    }};

    // try to close after all threads block
    // this wait does not guarantee that all threads will block by a dequeue, but even if they are not blocking yet,
    // they will return an invalid cart (i.e. close-then-dequeue and dequeue-then-close have the same result).
    std::this_thread::sleep_for(wait_time);
    queue.close();

    dequeue_thread.join();
}

TEST(multiple_item_cart_close_queue, no_producer_multiple_consumer_dequeue_after_close)
{
    using value_type = int;

    // close first then dequeue.

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    queue.close();

    for (int i = 0; i < 5; ++i) // TODO: this isn't really multiple consumer.
    {
        // should be non-blocking if queue was closed
        scq::cart_future<value_type> cart = queue.dequeue();

        EXPECT_FALSE(cart.valid());

        EXPECT_THROW(cart.get(), std::future_error);
    }
}

TEST(multiple_item_cart_close_queue, no_producer_multiple_consumer_release_blocking_dequeue_when_close)
{
    using value_type = int;

    // dequeue first then close.

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    // initialise 5 consuming threads
    std::vector<std::thread> dequeue_threads(5);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        return std::thread([&queue]
        {
            // should be blocking if queue was not yet closed
            scq::cart_future<value_type> cart = queue.dequeue();

            EXPECT_FALSE(cart.valid());

            EXPECT_THROW(cart.get(), std::future_error);
        });
    });

    // try to close after all threads block
    // this wait does not guarantee that all threads will block by a dequeue, but even if they are not blocking yet,
    // they will return an invalid cart (i.e. close-then-dequeue and dequeue-then-close have the same result).
    std::this_thread::sleep_for(wait_time);
    queue.close();

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();
}

TEST(multiple_item_cart_close_queue, single_producer_single_consumer_dequeue_after_close_process_full_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

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

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101}); // full-cart 1
    queue.enqueue(scq::slot_id{1}, value_type{102});
    queue.enqueue(scq::slot_id{1}, value_type{103}); // full-cart 2
    queue.enqueue(scq::slot_id{2}, value_type{200});
    queue.enqueue(scq::slot_id{2}, value_type{201}); // full-cart 3

    queue.close();

    // process full-carts
    for (int i = 0; i < 6 / 2; ++i)
    {
        // close allows to dequeue remaining elements
        scq::cart_future<value_type> cart = queue.dequeue();
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

TEST(multiple_item_cart_close_queue, single_producer_single_consumer_dequeue_after_close_process_half_filled_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {0, value_type{0}},
        {1, value_type{104}},
        {3, value_type{300}},
        {4, value_type{400}}
    };

    queue.enqueue(scq::slot_id{0}, value_type{0}); // half-filled cart
    queue.enqueue(scq::slot_id{1}, value_type{104}); // half-filled cart
    queue.enqueue(scq::slot_id{3}, value_type{300}); // half-filled cart
    queue.enqueue(scq::slot_id{4}, value_type{400}); // half-filled cart

    queue.close();

    // process half-filled carts
    for (int i = 0; i < 4; ++i)
    {
        // close allows to dequeue remaining elements
        scq::cart_future<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.second.size(), 1u);

        for (auto && value: cart_data.second)
        {
            EXPECT_TRUE(expected.cross_off({cart_data.first.slot_id, value}));
        }
    }

    // all results seen
    EXPECT_TRUE(expected.empty());
}

TEST(multiple_item_cart_close_queue, single_producer_single_consumer_dequeue_after_close_process_mixed_full_and_half_filled_carts)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{7}, scq::cart_capacity{2}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {0, value_type{0}}, // cart 1
        {1, value_type{100}},
        {1, value_type{101}}, // cart 2
        {1, value_type{102}},
        {1, value_type{103}}, // cart 3
        {1, value_type{104}}, // cart 4
        {2, value_type{200}}, // cart 5
        {2, value_type{201}},
        {3, value_type{300}}, // cart 6
        {4, value_type{400}} // cart 7
    };

    queue.enqueue(scq::slot_id{0}, value_type{0}); // half-filled cart 1
    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101}); // full-cart 1
    queue.enqueue(scq::slot_id{1}, value_type{102});
    queue.enqueue(scq::slot_id{1}, value_type{103}); // full-cart 2
    queue.enqueue(scq::slot_id{1}, value_type{104}); // half-filled cart 2
    queue.enqueue(scq::slot_id{2}, value_type{200});
    queue.enqueue(scq::slot_id{2}, value_type{201}); // full-cart 3
    queue.enqueue(scq::slot_id{3}, value_type{300}); // half-filled cart 3
    queue.enqueue(scq::slot_id{4}, value_type{400}); // half-filled cart 4

    queue.close();

    size_t full_cart_count{};
    size_t half_filled_cart_count{};

    // process full and half-filled carts
    for (int i = 0; i < 6 / 2 + 4; ++i)
    {
        // close allows to dequeue remaining elements
        scq::cart_future<value_type> cart = queue.dequeue();
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

    EXPECT_EQ(half_filled_cart_count, 4);
    EXPECT_EQ(full_cart_count, 3);

    // all results seen
    EXPECT_TRUE(expected.empty());
}

// TODO: add blocking enqueue test
// TODO: add maybe blocking enqueue / dequeue test
TEST(multiple_item_cart_close_queue, multiple_producer_multiple_consumer_release_blocking_dequeue_when_close)
{
    // this tests whether a blocking dequeue (= queue is empty) will be released by a close.

    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{2}, scq::cart_count{3}, scq::cart_capacity{2}};

    // count number of enqueues
    atomic_count enqueue_count{};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {0, value_type{100}},
        {0, value_type{101}}, // full-cart 1
        {0, value_type{102}},
        {0, value_type{103}}, // full-cart 2
        {1, value_type{200}} // half-filled cart 1
    };

    // initialise 6 producing threads
    std::vector<std::thread> enqueue_threads(6);
    std::generate(enqueue_threads.begin(), enqueue_threads.end(), [&]()
    {
        static size_t thread_id = 0;
        return std::thread([thread_id = thread_id++, &queue, &enqueue_count]
        {
            switch (thread_id)
            {
                case 0:
                    queue.enqueue(scq::slot_id{0}, value_type{100});
                    break;
                case 1:
                    queue.enqueue(scq::slot_id{0}, value_type{101});
                    break;
                case 2:
                    queue.enqueue(scq::slot_id{1}, value_type{200});
                    break;
                case 3:
                    queue.enqueue(scq::slot_id{0}, value_type{103});
                    break;
                case 4:
                    queue.enqueue(scq::slot_id{0}, value_type{102});
                    break;
            }

            // only first 5 threads actually insert into queue, but 6 threads arrive at the barrier
            ++enqueue_count;

            // barrier: wait until queue was closed (>= 7 means queue was closed)
            enqueue_count.wait_at_least(7);

            // queue should already be closed
            EXPECT_THROW(queue.enqueue(scq::slot_id{0}, value_type{0}), std::overflow_error);
        });
    });

    std::atomic_size_t half_filled_cart_count{};
    std::atomic_size_t full_cart_count{};

    // initialise 5 consuming threads
    std::vector<std::thread> dequeue_threads(5);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        return std::thread([&queue, &expected, &half_filled_cart_count, &full_cart_count]
        {
            while (true)
            {
                // should be blocking if queue was not yet closed
                scq::cart_future<value_type> cart = queue.dequeue();

                // abort if queue was closed
                if (!cart.valid())
                {
                    EXPECT_FALSE(cart.valid());
                    EXPECT_THROW(cart.get(), std::future_error);
                    break;
                }

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
        });
    });

    // barrier: wait until all elements are added
    enqueue_count.wait_at_least(6);

    // close after all producer threads are finished
    queue.close();

    ++enqueue_count; // signal that queue is closed

    for (auto && enqueue_thread: enqueue_threads)
        enqueue_thread.join();

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();

    EXPECT_EQ(half_filled_cart_count, 1);
    EXPECT_EQ(full_cart_count, 2);

    // all results seen
    EXPECT_TRUE(expected.empty());
}
