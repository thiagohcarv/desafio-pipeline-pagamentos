-- Evidência da carga determinística (seed=42)
SELECT (SELECT count(*) FROM customers)    AS customers,
       (SELECT count(*) FROM accounts)     AS accounts,
       (SELECT count(*) FROM cards)        AS cards,
       (SELECT count(*) FROM merchants)    AS merchants,
       (SELECT count(*) FROM transactions) AS transactions;
