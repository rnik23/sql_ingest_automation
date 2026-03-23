WITH cte_orders AS (
    SELECT o.OrderID,
           o.CustomerID,
           o.OrderDate,
           od.ProductID,
           od.Quantity,
           od.UnitPrice
    FROM dbo.Orders o
    INNER JOIN dbo.OrderDetails od ON o.OrderID = od.OrderID
    WHERE o.OrderDate >= '2025-01-01'
),
cte_customers AS (
    SELECT c.CustomerID,
           c.CustomerName,
           c.Region
    FROM dbo.Customers c
)
SELECT co.OrderID,
       cc.CustomerName,
       co.ProductID,
       co.Quantity,
       co.UnitPrice,
       (co.Quantity * co.UnitPrice) AS LineAmount
FROM cte_orders co
LEFT JOIN cte_customers cc ON co.CustomerID = cc.CustomerID;
