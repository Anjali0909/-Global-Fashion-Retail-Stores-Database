--- Creating Tables-----------
CREATE TABLE Discount (
    product_category VARCHAR2(100),
    sub_category VARCHAR2(100),
    start_date DATE,
    end_date DATE,
    discount NUMBER,
    PRIMARY KEY (product_category, sub_category)
);

CREATE TABLE Product (
    product_id NUMBER PRIMARY KEY,
    discount_category VARCHAR2(100),
    sub_category VARCHAR2(100),
    product_description VARCHAR2(255),
    color VARCHAR2(50),
    product_size VARCHAR2(50),
    FOREIGN KEY (discount_category, sub_category) REFERENCES Discount(discount_category, sub_category)
);

CREATE TABLE Customer (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR2(100),
    email VARCHAR2(100),
    city VARCHAR2(100),
    country VARCHAR2(100)
);

CREATE TABLE Store (
    store_id NUMBER PRIMARY KEY,
    country VARCHAR2(100),
    city VARCHAR2(100)
);

CREATE TABLE Employee (
    employee_id NUMBER PRIMARY KEY,
    store_id NUMBER,
    employee_name VARCHAR2(100),
    employee_position VARCHAR2(100),
    FOREIGN KEY (store_id) REFERENCES Store(store_id)
);

CREATE TABLE Transaction (
    invoice_id NUMBER,
    employee_id NUMBER,
    unit_price NUMBER,
    quantity NUMBER,
    invoice_total NUMBER,
    PRIMARY KEY (invoice_id, employee_id),
    FOREIGN KEY (invoice_id) REFERENCES Invoice(invoice_id),
    FOREIGN KEY (employee_id) REFERENCES Employee(employee_id)
);

------------------- Views----------------------

--1. View 1: Join at Least Three Tables and Add a WHERE Clause Using One of the Big Tables
CREATE VIEW employee_sales_performance AS
SELECT 
    s.store_id,
    s.country AS store_country,
    e.employee_name,
    SUM(t.quantity * t.unit_price) AS total_sales
FROM 
    store s
JOIN 
    employee e ON s.store_id = e.store_id
JOIN 
    transaction t ON e.employee_id = t.employee_id
WHERE 
    t.transaction_date >= TO_DATE('2023-01-01', 'YYYY-MM-DD')  -- Filter on transaction date
GROUP BY 
    s.store_id, s.country, e.employee_name;
 

--2. View 2: Join at Least Two Tables and a Subquery, Use Grouping with HAVING Using One of Your Big Tables  
CREATE VIEW region_sales_with_store_count AS
SELECT 
    s.country AS store_country, 
    COUNT(DISTINCT s.store_id) AS store_count,
    SUM(t.quantity * t.unit_price) AS total_sales
FROM 
    store s
JOIN 
    employee e ON s.store_id = e.store_id  
JOIN 
    transaction t ON e.employee_id = t.employee_id        
GROUP BY 
    s.country  
HAVING 
    COUNT(DISTINCT s.store_id) > 2;  -- Only include regions with more than 2 stores
    

-----3. Create indexes that can be used by your complex queries!

-- shrinking the space
ALTER TABLE customer ENABLE ROW MOVEMENT;

SELECT 
    segment_name, 
    segment_type, 
    ROUND(bytes / 1024 / 1024, 2) AS size_mb
FROM 
    dba_segments
WHERE 
    owner = 'GMNW8R' 
ORDER BY 
    size_mb DESC;
    
ALTER TABLE customer SHRINK SPACE;
COMMIT;


-- Partitioning by Year on transaction_date

CREATE TABLE transaction_partition (
    invoice_id VARCHAR2(50) PRIMARY KEY,
    employee_id NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    unit_price NUMBER,
    transaction_date DATE,
    invoice_total NUMBER
)
PARTITION BY RANGE (transaction_date) (
    PARTITION p_2023 VALUES LESS THAN (TO_DATE('2023-01-01', 'YYYY-MM-DD')),
    PARTITION p_2024 VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD')),
    PARTITION p_2025 VALUES LESS THAN (TO_DATE('2025-01-01', 'YYYY-MM-DD'))
);


--Indexes ----------------------------


-- before indexing 
EXPLAIN PLAN FOR
SELECT 
    invoice_id, 
    unit_price, 
    quantity, 
    invoice_total, 
    transaction_date
FROM 
    transaction
WHERE 
    customer_id = 123;
    
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());


--  1.   Index on customer_id:
CREATE INDEX idx_transaction_customer_id ON transaction(customer_id);

--	2.	Index on product_id:
CREATE INDEX idx_transaction_product_id ON transaction(product_id);

--  3.	Index on transaction_date:
CREATE INDEX idx_transaction_date ON transaction(transaction_date);

--	4.	Composite Index on customer_id and transaction_date:
CREATE INDEX idx_transaction_customer_date ON transaction(customer_id, transaction_date);



-- 4. Complex Queries Using the TRANSACTION Table -----

-- Query1:  Get all transactions for a specific customer
EXPLAIN PLAN FOR
SELECT 
    invoice_id, 
    unit_price, 
    quantity, 
    invoice_total, 
    transaction_date
FROM 
    transaction
WHERE 
    customer_id = 123;
    
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());


--Query 2: Get all transactions for a specific customer within a date range
EXPLAIN PLAN FOR
SELECT 
    invoice_id, 
    unit_price, 
    quantity, 
    invoice_total, 
    transaction_date
FROM 
    transaction
WHERE 
    customer_id = 123
    AND transaction_date BETWEEN TO_DATE('2025-01-01', 'YYYY-MM-DD') 
    AND TO_DATE('2025-12-31', 'YYYY-MM-DD');
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());




--- 5. Create a Stored PL/SQL Function------
CREATE FUNCTION calculate_total_transaction_value (p_customer_id IN NUMBER) 
RETURN NUMBER 
IS
    total_value NUMBER := 0;
    CURSOR transaction_cursor IS
        SELECT invoice_total 
        FROM transaction 
        WHERE customer_id = p_customer_id;
BEGIN
    -- Check if there are any transactions for the given customer
    IF p_customer_id IS NULL THEN
        RETURN 0;  -- Return 0 if the customer_id is invalid
    END IF;

    -- Loop through the cursor and accumulate the total value
    FOR trans_record IN transaction_cursor LOOP
        total_value := total_value + trans_record.invoice_total;
    END LOOP;

    -- If no transactions found, return 0
    IF total_value = 0 THEN
        RETURN 0;
    END IF;

    -- Return the calculated total value
    RETURN total_value;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;  -- If no data found for the customer, return 0
    WHEN OTHERS THEN
        RETURN NULL;  -- Return NULL in case of any other error
END;


-- Testing the function
SET SERVEROUTPUT ON;

DECLARE
    v_total NUMBER;
BEGIN
    v_total := calculate_total_transaction_value(773773);  -- an actual customer_id that exists
    DBMS_OUTPUT.PUT_LINE('Total Transaction Value: ' || v_total);
END;




--- 6. Create a Stored PL/SQL Procedure-----



--- Step1: sequencing 

CREATE SEQUENCE transaction_line_id_seq
START WITH 1
INCREMENT BY 1;

--- Step2: creating table
CREATE TABLE transaction_line_items (
    transaction_line_id NUMBER,  -- Unique line ID generated by sequence
    invoice_id VARCHAR2(50),  -- Foreign key to transaction (invoice)
    product_id NUMBER, 
    quantity NUMBER, 
    unit_price NUMBER, 
    line_total NUMBER, 
    PRIMARY KEY (transaction_line_id),
    FOREIGN KEY (invoice_id) REFERENCES transaction(invoice_id)
);

-- Step3: Procedure
CREATE PROCEDURE insert_transaction_line_item (
    p_invoice_id IN VARCHAR2, 
    p_product_id IN NUMBER, 
    p_quantity IN NUMBER, 
    p_unit_price IN NUMBER
)
IS
    v_transaction_line_id NUMBER;  -- Variable to hold the generated line ID
    v_line_total NUMBER := 0;  -- Variable to calculate the total for this line
    v_invoice_total NUMBER := 0;  -- Variable to hold the total invoice amount
BEGIN
    -- Step 1: Generate a unique transaction_line_id using the sequence
    SELECT transaction_line_id_seq.NEXTVAL INTO v_transaction_line_id FROM dual;

    -- Step 2: Calculate the line total (quantity * unit_price)
    v_line_total := p_quantity * p_unit_price;

    -- Step 3: Insert a new line item into the transaction_line_items table
    INSERT INTO transaction_line_items (transaction_line_id, invoice_id, product_id, quantity, unit_price, line_total)
    VALUES (v_transaction_line_id, p_invoice_id, p_product_id, p_quantity, p_unit_price, v_line_total);

    -- Step 4: Update the invoice total in the transaction table
    -- Calculate the updated total for the invoice
    SELECT SUM(line_total) INTO v_invoice_total
    FROM transaction_line_items
    WHERE invoice_id = p_invoice_id;

    -- Update the invoice total in the transaction table
    UPDATE transaction 
    SET invoice_total = v_invoice_total
    WHERE invoice_id = p_invoice_id;

    -- Commit the transaction
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- Rollback in case of any errors
        ROLLBACK;  
        -- Reraise the error to propagate it
        RAISE;     
END;


--- Step4: Procedure Execution

-- Calling the insert_transaction_line_item procedure for multiple products under the same invoice
BEGIN
    insert_transaction_line_item('INV001', 101, 5, 100);  -- Product ID 101, Quantity 5, Unit Price 100
    insert_transaction_line_item('INV001', 102, 2, 200);  -- Product ID 102, Quantity 2, Unit Price 200
END;
/



--- Triggers --------

-- 1. Trigger for Updating invoice_total:

CREATE TRIGGER update_invoice_total
AFTER INSERT ON transaction_line_items
FOR EACH ROW
DECLARE
    v_invoice_total NUMBER := 0;
BEGIN
    -- Calculate the updated invoice total for the corresponding invoice_id
    SELECT SUM(line_total) 
    INTO v_invoice_total
    FROM transaction_line_items
    WHERE invoice_id = :NEW.invoice_id;

    -- Update the invoice total in the transaction table
    UPDATE transaction
    SET invoice_total = v_invoice_total
    WHERE invoice_id = :NEW.invoice_id;
END;
/


-- Inserting line items into the transaction_line_items table will automatically update invoice_total
EXEC insert_transaction_line_item('INV001', 101, 5, 100);
EXEC insert_transaction_line_item('INV001', 102, 2, 200);

-- 2. Trigger for Deleting Line Items:

CREATE OR REPLACE TRIGGER update_invoice_total_on_delete
AFTER DELETE ON transaction_line_items
FOR EACH ROW
DECLARE
    v_invoice_total NUMBER := 0;
BEGIN
    -- Calculate the updated invoice total after deletion of a line item
    SELECT SUM(line_total) 
    INTO v_invoice_total
    FROM transaction_line_items
    WHERE invoice_id = :OLD.invoice_id;

    -- Update the invoice total in the transaction table
    UPDATE transaction
    SET invoice_total = v_invoice_total
    WHERE invoice_id = :OLD.invoice_id;
END;
/


-- Deleting a line item will automatically update the invoice_total
DELETE FROM transaction_line_items WHERE transaction_line_id = 1;


------ Granting Access -----


-- Grant SELECT, INSERT, UPDATE, DELETE on all the  tables
GRANT SELECT, INSERT, UPDATE, DELETE ON PRODUCT TO lkpeter;
GRANT SELECT, INSERT, UPDATE, DELETE ON TRANSACTION TO lkpeter;
GRANT SELECT, INSERT, UPDATE, DELETE ON CUSTOMER TO lkpeter;
GRANT SELECT, INSERT, UPDATE, DELETE ON DISCOUNT TO lkpeter;
GRANT SELECT, INSERT, UPDATE, DELETE ON EMPLOYEE TO lkpeter;
GRANT SELECT, INSERT, UPDATE, DELETE ON STORE TO lkpeter;


-- Grant EXECUTE on stored procedures and functions
GRANT EXECUTE ON insert_transaction_line_item TO lkpeter;
GRANT EXECUTE ON calculate_total_transaction_value TO lkpeter;

-- Grant SELECT on sequences
GRANT SELECT ON PRODUCT_SEQ TO lkpeter;
GRANT SELECT ON TRANSACTION_SEQ TO lkpeter;

-- Check the privileges granted to the user `lkpeter`
SELECT * FROM user_tab_privs WHERE grantee = 'LKPETER';