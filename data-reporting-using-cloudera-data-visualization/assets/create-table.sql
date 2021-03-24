CREATE DATABASE IF NOT EXISTS factory;
CREATE DATABASE IF NOT EXISTS reports;

DROP TABLE IF EXISTS factory.experimental_motor_enriched;
CREATE EXTERNAL TABLE factory.experimental_motor_enriched
  (  serial_no         STRING
   , vin               STRING
   , model             STRING
   , zip               INTEGER
   , customer_id       INTEGER
   , username          STRING
   , name              STRING
   , gender            CHAR(1)
   , email             STRING
   , occupation        STRING
   , birthdate         DATE
   , address           STRING
   , salary            FLOAT
   , sale_date         DATE
   , saleprice         FLOAT
   , latitude          STRING
   , longitude         STRING
   , factory_no        INTEGER
   , machine_no        INTEGER
   , part_no           STRING
   , local_timestamp   DOUBLE
   , status            STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (  'separatorChar' = ','
                      , 'quoteChar'  = '"')
-- IMPORTANT: Update <S3BUCKET> with the location of dataset and remove comment tag.
-- LOCATION '<S3BUCKET>'
tblproperties('skip.header.line.count' = '1');



-- LIST OF ALL BETA ENGINES
CREATE VIEW reports.beta_engines AS
  SELECT model, latitude, longitude
    FROM factory.experimental_motor_enriched;



-- RECALL NEEDED:
-- Parts created BETWEEN October 22 and October 24 were found
-- to be defective and need to be recalled.
CREATE VIEW reports.beta_engine_recall AS
  SELECT sale_date, model, vin, name, address, latitude, longitude
    FROM factory.experimental_motor_enriched
    WHERE sale_date BETWEEN CAST('2020-10-22' AS DATE) AND CAST('2020-10-24' AS DATE)
    ORDER BY sale_date DESC;
