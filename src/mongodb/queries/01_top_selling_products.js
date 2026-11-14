// AUCA E-Commerce Big Data Analytics Project
// Aggregation Query 1: Top 10 Products by Gross Revenue
// Business Question: Which products generate the highest sales revenue?

const db = db.getSiblingDB("ecommerce_analytics_full");

const results = db.transactions.aggregate([
    {
        $match: {
            status: { $in: ["completed", "delivered"] }
        }
    },
    {
        $unwind: "$items"
    },
    {
        $group: {
            _id: "$items.product_id",
            units_sold: { $sum: "$items.quantity" },
            transaction_count: { $sum: 1 },
            revenue: { $sum: "$items.subtotal" }
        }
    },
    {
        $lookup: {
            from: "products",
            localField: "_id",
            foreignField: "product_id",
            as: "product"
        }
    },
    {
        $unwind: "$product"
    },
    {
        $project: {
            _id: 0,
            product_id: "$_id",
            product_name: "$product.name",
            units_sold: 1,
            transaction_count: 1,
            revenue: { $round: ["$revenue", 2] }
        }
    },
    {
        $sort: { revenue: -1 }
    },
    {
        $limit: 10
    }
], { allowDiskUse: true }).toArray();

print("TOP 10 PRODUCTS BY GROSS REVENUE");
printjson(results);