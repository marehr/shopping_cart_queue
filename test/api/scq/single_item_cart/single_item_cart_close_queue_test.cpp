
#include <gtest/gtest.h>

#include <chrono>
#include <set>
#include <span>
#include <thread>

#include <scq/slotted_cart_queue.hpp>

#include "../atomic_count.hpp"
#include "../concurrent_cross_off_list.hpp"

static constexpr std::chrono::milliseconds wait_time(10);

TEST(single_item_cart_close_queue, no_producer_no_consumer_close_is_non_blocking)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.close();
}

TEST(single_item_cart_close_queue, single_producer_no_consumer_enqueue_after_close)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101});
    queue.enqueue(scq::slot_id{1}, value_type{102});
    queue.enqueue(scq::slot_id{1}, value_type{103});

    queue.close();

    EXPECT_THROW(queue.enqueue(scq::slot_id{2}, value_type{200}), std::overflow_error);
}

TEST(single_item_cart_close_queue, single_producer_no_consumer_release_blocking_enqueue_when_close)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // count number of enqueues
    atomic_count enqueue_count{};

    std::thread enqueue_thread{[&queue, &enqueue_count]
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

        // this should block
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

TEST(single_item_cart_close_queue, multiple_producer_no_consumer_enqueue_after_close)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

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

TEST(single_item_cart_close_queue, no_producer_single_consumer_dequeue_after_close)
{
    using value_type = int;

    // close first then dequeue.

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.close();

    // should be non-blocking if queue was closed (without close it would be blocking)
    scq::cart_future<value_type> cart = queue.dequeue();

    EXPECT_FALSE(cart.valid());

    EXPECT_THROW(cart.get(), std::future_error);
}

TEST(single_item_cart_close_queue, no_producer_single_consumer_release_blocking_dequeue_when_close)
{
    using value_type = int;

    // dequeue first then close.

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

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

TEST(single_item_cart_close_queue, no_producer_multiple_consumer_dequeue_after_close)
{
    using value_type = int;

    // close first then dequeue.

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.close();

    for (int i = 0; i < 5; ++i) // TODO: this isn't really multiple consumer.
    {
        // should be non-blocking if queue was closed
        scq::cart_future<value_type> cart = queue.dequeue();

        EXPECT_FALSE(cart.valid());

        EXPECT_THROW(cart.get(), std::future_error);
    }
}

TEST(single_item_cart_close_queue, no_producer_multiple_consumer_release_blocking_dequeue_when_close)
{
    using value_type = int;

    // dequeue first then close.

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

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

TEST(single_item_cart_close_queue, single_producer_single_consumer_dequeue_after_close)
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
        {2, value_type{200}}
    };

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101});
    queue.enqueue(scq::slot_id{1}, value_type{102});
    queue.enqueue(scq::slot_id{1}, value_type{103});
    queue.enqueue(scq::slot_id{2}, value_type{200});

    queue.close();

    for (int i = 0; i < 5; ++i)
    {
        // close allows to dequeue remaining elements
        scq::cart_future<value_type> cart = queue.dequeue();
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

// TODO: add blocking enqueue test
// TODO: add maybe blocking enqueue / dequeue test
TEST(single_item_cart_close_queue, multiple_producer_multiple_consumer_release_blocking_dequeue_when_close)
{
    // this tests whether a blocking dequeue (= queue is empty) will be released by a close.

    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // count number of enqueues
    atomic_count enqueue_count{};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    concurrent_cross_off_list<std::pair<std::size_t, value_type>> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}}
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

            // only first 5 threads actually insert into queue, but 6 threads arrive at the barrier
            ++enqueue_count;

            // barrier: wait until queue was closed (>= 7 means queue was closed)
            enqueue_count.wait_at_least(7);

            // queue should already be closed
            EXPECT_THROW(queue.enqueue(scq::slot_id{0}, value_type{0}), std::overflow_error);
        });
    });

    // initialise 5 consuming threads
    std::vector<std::thread> dequeue_threads(5);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        return std::thread([&queue, &expected]
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

                EXPECT_TRUE(expected.cross_off({
                    std::get<0>(cart_data).slot_id,
                    std::get<1>(cart_data)[0]
                }));
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

    // all results seen
    EXPECT_TRUE(expected.empty());
}
