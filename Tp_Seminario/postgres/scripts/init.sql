--CREATE SCHEMA workshop;

--DROP TABLE IF EXISTS stocks;
--CREATE TABLE stocks (
--  full_date timestamp NOT NULL,
--  symbol varchar(10) NOT NULL,
--  category varchar(64) NOT NULL,
--  open double precision	NOT NULL,
--  high double precision	NOT NULL,
--  low double precision	NOT NULL,
--  close double precision	NOT NULL,
--  MA20 double precision	NOT NULL,
--  MA50 double precision	NOT NULL,
--  MA100 double precision	NOT NULL,
--  PRIMARY KEY(full_date, symbol)
--);

CREATE SCHEMA workshop;

DROP TABLE IF EXISTS flights;
CREATE TABLE flights (
	FechaHora timestamp NOT NULL,
	Callsign varchar(10),
	Matricula varchar(10) NOT NULL,
	Aeronave_Cod varchar(6),
	Aerolinea_Cod varchar(3),
	Aerolinea_Nombre varchar(50),	
	Origen varchar(4) NOT NULL,
	Destino varchar(4) NOT NULL,
	PRIMARY KEY(FechaHora, Matricula)
);