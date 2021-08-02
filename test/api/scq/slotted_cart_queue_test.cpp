
#include <gtest/gtest.h>

#include <scq/slotted_cart_queue.hpp>

TEST(slotted_cart_queue_test, default_construct)
{
    scq::slotted_cart_queue<int> queue{};
}

TEST(slotted_cart_queue_test, valid_construct)
{
    scq::slotted_cart_queue<int> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};
}

TEST(slotted_cart_queue_test, invalid_construct)
{
    // capacity of a cart is too small (a cart should be able to store at least one item)
    EXPECT_THROW((scq::slotted_cart_queue<int>{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{0}}),
                 std::logic_error);

    // less carts than slots (would dead-lock)
    EXPECT_THROW((scq::slotted_cart_queue<int>{scq::slot_count{5}, scq::cart_count{1}, scq::cart_capacity{1}}),
                 std::logic_error);
}
