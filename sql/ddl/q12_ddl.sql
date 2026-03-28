-- Q12 [SQL | Senior]
-- Find the top 3 customers by total amount spent.
-- Return customer_id and total_spent, ordered by total_spent desc.
-- Ties at any rank are handled with dense ranking — all customers
-- within the top 3 dense rank positions must be returned.
-- Output columns: customer_id, total_spent

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

PRINT '╔══════════════════════════════════════════════════════════╗';
GO
PRINT '║ Q12 [SQL | Coding | Senior] -- Find the top 3 customers ║';
GO
PRINT '║ by total amount spent. Return customer_id and           ║';
GO
PRINT '║ total_spent, ordered by total_spent descending. If      ║';
GO
PRINT '║ Ties at any rank use dense ranking — return all          ║';
GO
PRINT '║ customers within the top 3 dense rank positions.       ║';
GO
PRINT '╠══════════════════════════════════════════════════════════╣';
GO
PRINT '║ Table : orders                                           ║';
GO
PRINT '║ Cols  : order_id    INT                                  ║';
GO
PRINT '║         customer_id INT                                  ║';
GO
PRINT '║         order_date  DATE                                 ║';
GO
PRINT '║         amount      DECIMAL(10,2)                        ║';
GO
PRINT '║ Output: customer_id, total_spent                         ║';
GO
PRINT '╚══════════════════════════════════════════════════════════╝';
GO

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id    INT           NOT NULL,
    customer_id INT           NOT NULL,
    order_date  DATE          NOT NULL,
    amount      DECIMAL(10,2) NOT NULL
);
GO

INSERT INTO orders (order_id, customer_id, order_date, amount) VALUES
    (1, 1, '2024-01-01', 500.00),
    (2, 1, '2024-01-05', 300.00),
    (3, 2, '2024-01-02', 1200.00),
    (4, 3, '2024-01-03', 400.00),
    (5, 3, '2024-01-04', 200.00),
    (6, 4, '2024-01-06', 150.00),
    (7, 5, '2024-01-07', 900.00);
GO
