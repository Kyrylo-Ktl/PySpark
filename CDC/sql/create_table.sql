CREATE SCHEMA cdc_schema;
CREATE TABLE cdc_schema.Persons
(
    PersonID int,
    FullName varchar(255),
    City     varchar(255),
    PRIMARY KEY (PersonID)
);