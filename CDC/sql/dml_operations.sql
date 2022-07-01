UPDATE cdc_schema.Persons SET FullName = 'ABC XYZ' WHERE PersonId = 7;
INSERT INTO cdc_schema.Persons VALUES (130,'Alica Bing','New York');
INSERT INTO cdc_schema.Persons VALUES (131,'Malinda Bing','Detroit');
INSERT INTO cdc_schema.Persons VALUES (132,'Chandler Bing','Portland');
UPDATE cdc_schema.Persons SET FullName = 'ABC XYZ' WHERE PersonId = 8;
DELETE FROM cdc_schema.Persons WHERE PersonId = 10;