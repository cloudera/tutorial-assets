DROP TABLE IF EXISTS default.shipping;


CREATE EXTERNAL TABLE IF NOT EXISTS default.shipping (
    ID                  integer,
    Warehouse_block     string,
    Mode_of_Shipment    string,
    Customer_care_calls integer,
    Customer_rating     integer,
    Cost_of_the_Product integer,
    Prior_purchases     integer,
    Product_importance  string,
    Gender              string,
    Discount_offered    string,
    Weight_in_gms       integer,
    Arrive_on_time      integer
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION ${dataset_location}
  tblproperties("skip.header.line.count"="1");


SELECT * FROM default.shipping;