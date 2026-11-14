// Run in mongosh after loading data: use ecommerce_analytics
// Aggregation 1: top products by completed sales (net revenue allocated after discounts)
db.transactions.aggregate([
 {$match:{status:'completed'}},{$unwind:'$items'},
 {$project:{product_id:'$items.product_id',units:'$items.quantity',netRevenue:{$cond:[{$gt:['$subtotal',0]},{$multiply:['$items.subtotal',{$subtract:[1,{$divide:['$discount','$subtotal']}]}]},'$items.subtotal']}}},
 {$group:{_id:'$product_id',units:{$sum:'$units'},revenue:{$sum:'$netRevenue'}}},
 {$sort:{revenue:-1}},{$limit:10}
]);
// Aggregation 2: repeat-customer behavior
 db.transactions.aggregate([
 {$match:{status:'completed'}},
 {$group:{_id:'$user_id',orders:{$sum:1},spend:{$sum:'$total'}}},
 {$bucket:{groupBy:'$orders',boundaries:[1,2,3,1000000],default:'other',output:{customers:{$sum:1},totalSpend:{$sum:'$spend'}}}}
]);
