// AUCA E-Commerce Big Data Analytics Project
// Aggregation Query 2: Top 10 Categories by Gross Revenue
// Business Question: Which product categories generate the most revenue?

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
        $lookup: {
            from: "products",
            localField: "items.product_id",
            foreignField: "product_id",
            as: "product"
        }
    },
    {
        $unwind: "$product"
    },
    {
        $lookup: {
            from: "categories",
            localField: "product.category_id",
            foreignField: "category_id",
            as: "category"
        }
    },
    {
        $unwind: "$category"
    },
    {
        $group: {
            _id: "$category.name",
            units_sold: { $sum: "$items.quantity" },
            gross_revenue: { $sum: "$items.subtotal" }
        }
    },
    {
        $sort: { gross_revenue: -1 }
    },
    {
        $limit: 10
    },
    {
        $project: {
            _id: 0,
            category: "$_id",
            units_sold: 1,
            gross_revenue: { $round: ["$gross_revenue", 2] }
        }
    }
], { allowDiskUse: true }).toArray();

print("TOP 10 PRODUCT CATEGORIES BY GROSS REVENUE");
printjson(results);