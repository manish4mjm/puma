CREATE SCHEMA "ref" AUTHORIZATION postgres;
CREATE SCHEMA "time_series" AUTHORIZATION postgres;

DROP TABLE "ref".equity;

CREATE TABLE "ref".equity (
	equity_id serial NOT NULL,
	comp_name varchar NOT NULL,
	sector varchar NOT NULL,
	local_code varchar NOT NULL,
	ric_code varchar NOT NULL,
	ds_code varchar NOT NULL,
	exchange varchar NOT NULL,
	currency varchar NOT null,
	CONSTRAINT equity_pkey PRIMARY KEY (equity_id)
)
INHERITS ("ref".last_updated_base);


DROP TABLE "time_series".equity_eod_data;

CREATE TABLE "time_series".equity_eod_data (
	equity_id serial NOT NULL,
	trading_date date not null,
	data_source varchar NOT NULL,
	open numeric(36,15) NOT NULL,
	high numeric(36,15) NOT NULL,
	low numeric(36,15) NOT NULL,
	close numeric(36,15) NOT NULL,
	adj_close numeric(36,15) NOT NULL,
	CONSTRAINT equity_id_fkey FOREIGN KEY (equity_id) REFERENCES ref.equity(equity_id)
)
INHERITS ("ref".last_updated_base);

CREATE TABLE "time_series".equity_indicators (
	equity_id serial NOT NULL,
	trading_date date not null,
	data_source varchar NOT NULL,
	rsi_6 numeric(36,15) NOT NULL,
	rsi_14 numeric(36,15) NOT NULL,
	boll_up numeric(36,15) NOT NULL,
	boll_lw numeric(36,15) NOT NULL,
	ema_200 numeric(36,15) NOT NULL,
	ema_100 numeric(36,15) NOT NULL,
	ema_50 numeric(36,15) NOT NULL,
	macd numeric(36,15) NOT NULL,
	CONSTRAINT equity_id_fkey FOREIGN KEY (equity_id) REFERENCES ref.equity(equity_id)
)
INHERITS ("ref".last_updated_base);


DROP TABLE "time_series".equity_anr_data;

CREATE TABLE "time_series".equity_anr_data (
	equity_id serial NOT NULL,
	trading_date date not null,
	anr_count numeric(36,15) NOT NULL,
	anr_reco numeric(36,15) NOT NULL,
	anr_med numeric(36,15) NOT NULL,
	CONSTRAINT equity_id_fkey FOREIGN KEY (equity_id) REFERENCES ref.equity(equity_id)
)
INHERITS ("ref".last_updated_base);



-- Drop table

-- DROP TABLE "ref".last_updated_base;

CREATE TABLE "ref".last_updated_base (
	last_updated_by varchar(128) NOT NULL DEFAULT "session_user"(),
	last_updated_timestamp timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT last_updated_base_no_insert CHECK (false) NO INHERIT
);

