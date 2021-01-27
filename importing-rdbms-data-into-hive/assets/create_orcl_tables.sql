CREATE TABLE LOCATION
(
    location_id     number(10)    NOT NULL,
    location_name   varchar2(50)  NOT NULL,
    CONSTRAINT location_pk PRIMARY KEY (location_id)
);

CREATE TABLE VACCINE
(
    vaccine_id    number(10)    NOT NULL,
    vaccine_name  varchar2(50)  NOT NULL,
    vaccine_dose  number(10),
    vaccine_age   varchar2(50),
    CONSTRAINT vaccine_pk PRIMARY KEY (vaccine_id)
);


CREATE TABLE VACCINATION_RATE
(
    location_id         number(10)    NOT NULL,
    vaccine_id          number(10)    NOT NULL,
    year                number(10)    NOT NULL,
    rate                decimal(10,1),
    lower_limit         decimal(10,1),
    upper_limit         decimal(10,1),
    confidence_interval decimal(10,1),
    sample_size         number(20),
    target              decimal(10,1),
    CONSTRAINT vaccination_rate_pk PRIMARY KEY (location_id, vaccine_id, year)
);

commit;