DELIMITER $$

DROP PROCEDURE IF EXISTS explode_tableYW $$
CREATE PROCEDURE explode_tableYW(bound VARCHAR(255))

  BEGIN

    DECLARE A text ;
    DECLARE B TEXT;
    DECLARE occurance INT DEFAULT 0;
    DECLARE i INT DEFAULT 0;
    DECLARE splitted_value text;
    DECLARE done INT DEFAULT 0;
    DECLARE cur1 CURSOR FOR SELECT safeeventa.setA, safeeventa.setB
                                         FROM safeeventa
                                                      ;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    DROP TEMPORARY TABLE IF EXISTS bpi_2014.safea;
    CREATE TEMPORARY TABLE bpi_2014.safea(
    `setA` VARCHAR(255) NOT NULL,
    `setB` VARCHAR(255) NOT NULL
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

          INSERT INTO bpi_2014.safea VALUES (A,splitted_value);
          SET i = i + 1;
        END WHILE;
      END LOOP;

      
    CLOSE cur1;
  END; $$