

select User, HOst from mysql.user;

CREATE USER 'ds'@'localhost' IDENTIFIED BY 'password';
drop user 'ds'@'localhost';

GRANT ALL PRIVILEGES ON *.* TO 'ds'@'localhost' WITH GRANT OPTION;
REVOKE ALL PRIVILEGES ON *.* FROM 'ds'@'localhost';

sudo mysql
mysql -u keycloak -p
