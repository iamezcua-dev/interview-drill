-- Q5 [SQL | Senior]: Find customers who placed orders in every
--     month of 2024.
-- Output columns: customer_id

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

PRINT '╔══════════════════════════════════════════════════════════╗';
GO
PRINT '║ Q5 [SQL | Coding | Senior] -- Find customers who placed ║';
GO
PRINT '║ orders in every month of 2024.                          ║';
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
PRINT '║ Output: customer_id                                      ║';
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

INSERT INTO orders VALUES
    (1,  1, '2024-01-15', 100),
    (2,  1, '2024-02-10', 200),
    (3,  1, '2024-03-05', 150),
    (4,  1, '2024-04-20', 300),
    (5,  1, '2024-05-11', 250),
    (6,  1, '2024-06-30', 175),
    (7,  1, '2024-07-14', 400),
    (8,  1, '2024-08-22', 320),
    (9,  1, '2024-09-09', 210),
    (10, 1, '2024-10-18', 190),
    (11, 1, '2024-11-25', 280),
    (12, 1, '2024-12-31', 350),
    (13, 2, '2024-01-05', 100),
    (14, 2, '2024-02-14', 200),
    (15, 2, '2024-03-22', 150),
    (16, 3, '2024-01-10', 500),
    (17, 3, '2024-01-28', 600);
GO
