SELECT Outlet_Type, SUM(Item_Outlet_Sales)
FROM bigmart_sales
GROUP BY Outlet_Type;
