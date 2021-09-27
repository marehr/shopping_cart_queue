#include <gtest/gtest.h>

#include <set>

#include <scq/slotted_cart_queue.hpp>

#include "../concurrent_cross_off_list.hpp"

TEST(single_item_cart_sequential, single_enqueue_dequeue)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.enqueue(scq::slot_id{1}, value_type{100});

    // item enqueued will be available immediately. (cart can contain only one item)
    {
        scq::cart_future<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.first.slot_id, 1);
        EXPECT_EQ(*cart_data.second.begin(), value_type{100});
    }

    queue.enqueue(scq::slot_id{2}, value_type{200});

    // item enqueued will be available immediately. (cart can contain only one item)
    {
        scq::cart_future<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.first.slot_id, 2);
        EXPECT_EQ(*cart_data.second.begin(), value_type{200});
    }
}

TEST(single_item_cart_sequential, multiple_enqueue_dequeue)
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

    for (int i = 0; i < 5; ++i)
    {
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
