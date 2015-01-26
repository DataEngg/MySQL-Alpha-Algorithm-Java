DELIMITER $$
DROP PROCEDURE IF EXISTS explode_table_for_place1 $$
CREATE PROCEDURE explode_table_for_place1(bound VARCHAR(255))
  BEGIN
    DECLARE A text ;
    DECLARE B TEXT;
    DECLARE occurance INT DEFAULT 0;
    DECLARE i INT DEFAULT 0;
    DECLARE splitted_value text;
    DECLARE done INT DEFAULT 0;
    DECLARE cur1 CURSOR FOR SELECT place1.id, place1.value FROM place1  ;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    DROP TABLE IF EXISTS bpi_2014.temp_place1;
    CREATE TABLE bpi_2014.temp_place1(
    `id` VARCHAR(255) NOT NULL,
    `value` VARCHAR(255) NOT NULL );
    OPEN cur1;
      read_loop: LOOP
        FETCH cur1 INTO A, B;
        IF done THEN
          LEAVE read_loop;
        END IF;
        SET occurance = (SELECT LENGTH(A)- LENGTH(REPLACE(A, bound, ''))+1);
        SET i=1;
        WHILE i <= occurance DO
          SET splitted_value =
          (SELECT REPLACE(SUBSTRING(SUBSTRING_INDEX(A, bound, i),
          LENGTH(SUBSTRING_INDEX(A, bound, i - 1)) + 1), ',', ''));
          INSERT INTO bpi_2014.temp_place1 VALUES (splitted_value,B);
          SET i = i + 1;
        END WHILE;
      END LOOP;
      SELECT distinct id,value FROM bpi_2014.temp_place1;
    CLOSE cur1;
  END; $$
