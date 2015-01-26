DELIMITER $$

DROP PROCEDURE IF EXISTS dummy_table $$
CREATE PROCEDURE dummy_table(bound VARCHAR(255))

  BEGIN

    DECLARE A text ;
    DECLARE B TEXT;
    DECLARE occurance INT DEFAULT 0;
    DECLARE i INT DEFAULT 0;
    DECLARE splitted_value text;
    DECLARE done INT DEFAULT 0;
    DECLARE cur1 CURSOR FOR SELECT dummy.setA, dummy.setB
                                         FROM dummy
                                                      ;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    DROP TEMPORARY TABLE IF EXISTS bpi_2013.dummylast;
    CREATE TEMPORARY TABLE bpi_2013.dummylast(
    `id` VARCHAR(255) NOT NULL,
    `value` VARCHAR(255) NOT NULL
    );

    OPEN cur1;
      read_loop: LOOP
        FETCH cur1 INTO A, B;
        IF done THEN
          LEAVE read_loop;
        END IF;

        SET occurance = (SELECT LENGTH(B)
                                 - LENGTH(REPLACE(B, bound, ''))
                                 +1);
        SET i=1;
        WHILE i <= occurance DO
          SET splitted_value =
          (SELECT REPLACE(SUBSTRING(SUBSTRING_INDEX(B, bound, i),
          LENGTH(SUBSTRING_INDEX(B, bound, i - 1)) + 1), ',', ''));

          INSERT INTO bpi_2013.dummylast VALUES (A,splitted_value);
          SET i = i + 1;
        END WHILE;
      END LOOP;

      SELECT distinct id,value FROM bpi_2013.dummylast;
    CLOSE cur1;
  END; $$