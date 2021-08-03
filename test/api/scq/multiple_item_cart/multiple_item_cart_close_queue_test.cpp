
#include <gtest/gtest.h>

#include <chrono>
#include <set>
#include <span>
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

