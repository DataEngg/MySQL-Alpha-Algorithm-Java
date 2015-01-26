DELIMITER $$

DROP PROCEDURE IF EXISTS explode_tableYW1 $$
CREATE PROCEDURE explode_tableYW1(bound VARCHAR(255))
  BEGIN
    DECLARE A text ;
    DECLARE B TEXT;
    DECLARE occurance INT DEFAULT 0;
    DECLARE i INT DEFAULT 0;
    DECLARE splitted_value text;
    DECLARE done INT DEFAULT 0;
    DECLARE cur1 CURSOR FOR SELECT safeeventb.setA, safeeventb.setB
                                         FROM safeeventb
;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    DROP TEMPORARY TABLE IF EXISTS bpi_2014.safeb;
    CREATE TEMPORARY TABLE bpi_2014.safeb(
    `setA` VARCHAR(255) NOT NULL,
    `setB` VARCHAR(255) NOT NULL
    );

    OPEN cur1;
      read_loop: LOOP
        FETCH cur1 INTO A, B;
        IF done THEN
          LEAVE read_loop;
        END IF;

        SET occurance = (SELECT LENGTH(A)
                                 - LENGTH(REPLACE(A, bound, ''))
                                 +1);
        SET i=1;
        WHILE i <= occurance DO
          SET splitted_value =
          (SELECT REPLACE(SUBSTRING(SUBSTRING_INDEX(A, bound, i),
          LENGTH(SUBSTRING_INDEX(A, bound, i - 1)) + 1), ',', ''));

          INSERT INTO bpi_2014.safeb VALUES (splitted_value,B);
          SET i = i + 1;
        END WHILE;
      END LOOP;

      SELECT distinct setA,setB FROM bpi_2014.safeb;
    CLOSE cur1;
  END; $$