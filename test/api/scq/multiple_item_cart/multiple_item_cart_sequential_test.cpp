#include <gtest/gtest.h>

#include <set>
#include <span>

#include <scq/slotted_cart_queue.hpp>

#include "../concurrent_cross_off_list.hpp"

TEST(multiple_item_cart_sequential, single_cart_enqueue_dequeue)
{
    using value_type = int;

    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{2}};

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101});

    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.first.slot_id, 1);
        EXPECT_EQ(cart_data.second.size(), 2);
        EXPECT_EQ(cart_data.second[0], value_type{100});
        EXPECT_EQ(cart_data.second[1], value_type{101});
    }

    queue.enqueue(scq::slot_id{2}, value_type{200});
    queue.enqueue(scq::slot_id{2}, value_type{201});

    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.first.slot_id, 2);
        EXPECT_EQ(cart_data.second.size(), 2);
        EXPECT_EQ(cart_data.second[0], value_type{200});
        EXPECT_EQ(cart_data.second[1], value_type{201});
    }
}

TEST(multiple_item_cart_sequential, multiple_enqueue_dequeue)
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

    queue.enqueue(scq::slot_id{1}, value_type{103});
    queue.enqueue(scq::slot_id{2}, value_type{200});
    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101});
    queue.enqueue(scq::slot_id{2}, value_type{201});
    queue.enqueue(scq::slot_id{1}, value_type{102});

    for (int i = 0; i < 6 / 2; ++i)
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
