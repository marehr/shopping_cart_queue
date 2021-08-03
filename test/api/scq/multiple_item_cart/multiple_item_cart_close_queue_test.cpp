
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

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    queue.close();
}

TEST(multiple_item_cart_close_queue, single_producer_no_consumer_enqueue_after_close)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101}); // one full cart

    queue.enqueue(scq::slot_id{1}, value_type{102}); // one half-filled cart

    queue.close();

    EXPECT_THROW(queue.enqueue(scq::slot_id{2}, value_type{200}), std::overflow_error);
}

